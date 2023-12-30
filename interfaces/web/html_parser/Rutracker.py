
from decimal import Decimal

from interfaces.web.html_parser.Parser import HtmlParser
#typing
from typing import List, Tuple
from bs4 import BeautifulSoup, Tag

FORUMS = [
   ("797", 10),   # Electro lossless
   ("909", 109),  # Foreign hip hop, rap lossless
   ("1486", 34),  # Domestic hip hop, rap lossless
   ("1665", 31),  # Foreign RnB lossless
   ("1768", 44),  # Reggae, dancehall, dub lossless
   ("1818", 198), # Trance lossless
   ("1825", 114), # Techno lossless
   ("1829", 34),  # Hardcore, hardstyle, jumpstyle lossless
   ("1832", 248), # Drum & bass, jungle lossless
   ("1836", 39),  # Breakbeat lossless
   ("1839", 28),  # Dubstep lossless
   ("1840", 54),  # IDM lossless
   ("1857", 325), # House lossless
   ("1864", 153), # Traditional electronic, ambient lossless
   ("1868", 57),  # EBM, dark electro, aggrotech lossless
   ("1869", 106), # Experimental lossless
   ("1945", 38)   # Trip hop lossless
]


    #def parse_page_html(self, path: str) -> List[Dict]:
    #    with closing(path, "r") as _f:
    #        _html = _f.read()
    #    soup = self.page_soup(_html)
    #    for _row in self.row_items(soup):
    #        row_text = self.row_text(_row)
    #        tag_str, _ = self.split_tags_title(row_text)
    #        tags = self.tags_from_tag_str(tag_str)



class Parser(HtmlParser):

    def __init__(self):
        super().__init__()

    def row_items(self, forum_page_soup: BeautifulSoup) -> List[Tag]:
        wrapper = forum_page_soup.find("table", {"class": "forum"})
        return wrapper.find("tbody").find_all("tr", {"class": "hl-tr"})

    def row_text(self, row_item: Tag) -> str:
        return row_item.find("a", {"class": "tt-text"}).text
    
    def size(self, row_item: Tag) -> str:
        size_text = row_item.find("td", {"class": "tor-size"}).find("a").text
        size_str = size_text.split(" ")[0].split("\xa0")[0]
        if "GB" in size_text:
            return str(Decimal(size_str) * 1000)[:-3]
        return size_str

    def site_id(self, row_item: Tag) -> str:
        wrapper = row_item.find("div", {"class": "t-title"})
        return wrapper.find("a").attrs["href"].split("t=")[-1]
    
    def download_count(self, result_row: Tag) -> str:
        count = result_row.find("td", {"class": "number-format"})
        return count.text if count else ""

    def pages(self, soup: BeautifulSoup) -> List[str]:
        title_tag = soup.find("h1", {"class": "maintitle"})
        wrapper = title_tag.parent()
        page_links = wrapper[1].find_all("a", {"class": "pg"})
        if not page_links:
            return []
        return [_a.attrs["href"] for _a in page_links if _a.text.isnumeric()]


class ForumPageParser(Parser):

    def __init__(self):
        super().__init__()

    def split_tags_title(self, row_text: str) -> Tuple[List[str], str]:
        if not row_text.startswith("("):
            return ("", row_text)
        parts = row_text.split(") ")
        tag_str, title = parts[0][1:], ") ".join(parts[1:])
        tags = [_t.lower() for _t in tag_str.split(", ")]
        return (tags, title)

    def tags_from_tag_str(self, tag_str: str) -> List[str]:
        delims = [",", "|", "/"]
        split_at = [_d for _d in delims if _d in tag_str]
        match len(split_at):
            case 0:
                return [tag_str]
            case 1:
                return [_t.strip() for _t in tag_str.split(split_at)]
            case _:
                if "," in split_at:
                    return [_t.strip() for _t in tag_str.split(",")]
                else:
                    return [_t.strip() for _t in tag_str.split("|")]
    

class SearchResultsParser(Parser):

    def __init__(self):
        super().__init__()

    def row_items(self, soup: BeautifulSoup) -> List[Tag]:
        results = soup.find("div", {"id": "search-results"})
        rows = results.find("table").find("tbody").find_all("tr")
        return rows
    
    def tags(self, result_row: Tag) -> List[str]:
        wrapper = result_row.find("div", {"class": "t-tags"})
        tag_tags = wrapper.find_all("span", {"class": "tg"}) if wrapper else []
        return [_t.text for _t in tag_tags] if tag_tags else []



from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS rowitem (
  id integer PRIMARY KEY,
  forum integer NOT NULL,
  page integer NOT NULL,
  item text NOT NULL,
  size text NOT NULL,
  site_id text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS tag (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS rowitemtags (
  id integer PRIMARY KEY,
  rowitem integer NOT NULL,
  tag integer NOT NULL,
  FOREIGN KEY (rowitem)
    REFERENCES rowitem (id)
    ON DELETE CASCADE,
  FOREIGN KEY (tag)
    REFERENCES tag (id)
    ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS log (
  id integer PRIMARY KEY,
  filename text NOT NULL
)
"""
]

class Data(DatabaseInterface):

    def __init__(self, dbpath=""):
        super().__init__(dbpath)
        self.tag_cache = {}
        self.site_id_cache = []

  # Row Item Methods
    def new_row_item(self, forum, page, row_text, size, site_id):
        self.c.execute("INSERT INTO rowitem (forum, page, item, size, site_id)"\
                       "VALUES (?,?,?,?);",
                       (forum, page, row_text, size, site_id))
        self.c.execute("SELECT last_insert_rowid();")
        return self.c.fetchone()[0]
    
    def row_item_id(self, row_item_site_id) -> str:
        self.c.execute("SELECT id FROM rowitem WHERE site_id = ?;",
                       (row_item_site_id))
        result = self.c.fetchone()
        return result[0] if result else ""

  # Tag Methods
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

    def new_row_item_tag(self, row_item_id, tag_id):
        self.c.execute("INSERT INTO labeltags (result, tag) VALUES (?,?)",
                       (row_item_id, tag_id))
        return True

  # Site ID Cache
    def populate_site_id_cache(self) -> List[str]:
        self.c.execute("SELECT site_id FROM rowitem;")
        return self.c.fetchall()
    
    def row_item_exists(self, site_id) -> bool:
        return site_id in self.site_id_cache

  # HTML File Log Methods
    def log_new_file(self, file_name: str) -> bool:
        self.c.execute("INSERT INTO log (filename) VALUES (?);", (file_name,))
        self.db.commit()
        return True
    
    def file_exists(self, file_name: str) -> bool:
        self.c.execute("SELECT id FROM log WHERE filename = ?;", (file_name,))
        return bool(self.c.fetchone())

