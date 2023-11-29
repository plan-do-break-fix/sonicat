
from bs4 import BeautifulSoup
from dataclasses import dataclass
from decimal import Decimal

from interfaces.scrapers.Scraper import WebScraper
from util.NameUtility import NameUtility

#typing
from typing import Dict, List, Tuple
from bs4 import Tag


@dataclass
class Result:
    name: str
    site_id: str
    download_count: str
    tags=[]


class Scraper(WebScraper):

    def __init__(self):
        super().__init__()

    def query_url(self, query_text, format="flac") -> str:
        query = self.html_encode(f"{query_text} {format}")
        return f"https://rutracker.org/forum/tracker.php?nm={query}"

    def result_rows(self, soup: BeautifulSoup) -> List[Tag]:
        results = soup.find("div", {"id": "search-results"})
        rows = results.find("table").find("tbody").find_all("tr")
        return rows
    
    def name(self, result_row: Tag) -> str:
        name = result_row.find("div", {"class": "t-title"})
        return name if name else ""

    def tags(self, result_row: Tag) -> List[str]:
        tags_wrapper = result_row.find("div", {"class": "t-tags"})
        tag_tags = tags_wrapper.find_all("span", {"class": "tg"}) if tags_wrapper else []
        return [_t.text for _t in tag_tags] if tag_tags else []

    def download_count(self, result_row: Tag) -> str:
        count = result_row.find("td", {"class": "number-format"})
        return count.text if count else ""

    def site_id(self, result_row: Tag) -> str:
        return result_row.attrs["data-topic_id"]
    
    def size(self, result_row: Tag) -> str:
        size_text = result_row.find("td", {"class": "tor-size"}).find("a").text
        size_str = size_text.split(" ")[0]
        if "GB" in size_text:
            return str(Decimal(size_str) * 1000)
        return size_str


    def pages(self, soup: BeautifulSoup) -> List[str]:
        page_links = soup.find("h1", {"class": "maintitle"})\
                         .parent()\
                         .find("p", {"class": "small"})\
                         .find_all("a", {"class": "pg"})
        if not page_links:
            return []
        return [_a.attrs["href"] for _a in page_links if _a.isnumeric()]



from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS result (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  asset integer NOT NULL,
  name text NOT NULL,
  site_id text NOT NULL,
  size text NOT NULL,
  downloads text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS failedsearch (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  asset integer NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS tag (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS tags (
  id integer PRIMARY KEY,
  result integer NOT NULL,
  tag integer NOT NULL,
  FOREIGN KEY (result)
    REFERENCES result (id)
    ON DELETE CASCADE,
  FOREIGN KEY (tag)
    REFERENCES tag (id)
    ON DELETE CASCADE
);
"""
]

class Data(DatabaseInterface):

    def __init__(self, dbpath=""):
        super().__init__(dbpath)
        self.tag_cache = {}

    def record_result(self, catalog, asset_id, res: Result) -> bool:
        result_id = self.new_result(catalog, asset_id, res)
        if res.tags:
            for tag in res.tags:
                tag_id = self.get_cached_tag_id_with_insertion(tag)
                self.new_result_tag(result_id, tag_id)
        self.db.commit()
        return True

    def new_result(self, catalog, asset_id, res: Result) -> str:
        query = "INSERT INTO result (catalog, asset, name, site_id, size, downloads)"\
                "VALUES (?,?,?,?,?,?)"
        arguments = [catalog, asset_id, res.name, res.site_id, res.download_count]
        self.c.execute(query, arguments)
        self.c.execute("SELECT last_insert_rowid();")
        return self.c.fetchone()[0]

    def get_cached_tag_id_with_insertion(self, tag: str) -> str:
        tag = tag.lower()
        if tag not in self.tag_cache:
            self.c.execute('SELECT id FROM tag WHERE name = ?;', (tag,))
            result = self.c.fetchone()
            if not result:
                self.c.execute("INSERT INTO tag (name) VALUES (?);", (tag,))
                self.c.execute("SELECT last_insert_rowid();")
                result = self.c.fetchone()
            self.tag_cache[tag] = result[0]
        return self.tag_cache[tag]

    def new_result_tag(self, result_id, tag_id):
        self.c.execute("INSERT INTO tags (result, tag) VALUES (?,?)",
                       (result_id, tag_id))
        return True