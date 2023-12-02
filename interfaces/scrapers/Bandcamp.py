

from bs4 import BeautifulSoup
from dataclasses import dataclass
from decimal import Decimal
import re

from interfaces.scrapers.Scraper import Scraper

#typing
from typing import Dict, List, Tuple
from bs4 import Tag

@dataclass
class ParsedAlbum:
    title: str
    artist = ""
    publisher = ""
    year = ""
    description = ""
    tags = []
    tracks = []
    reviews = []
    api_id = ""
    api_url=""
    n_tracks = ""

    def track_durations(self) -> List[int]:
        return [_t.duration for _t in self.tracks]

@dataclass
class ParsedTrack:
    title: str
    duration = ""


class WebScraper(Scraper):

    def __init__(self):
        super().__init__()

    def query_url(self, query_text, format="flac") -> str:
        query = self.html_encode(f"{query_text} {format}")
        return f"https://bandcamp.com/search?q={query}&item_type=a"

    def result_rows(self, soup: BeautifulSoup) -> List[Tag]:
        rows = soup.find("li", {"class": "searchresult"})
        return rows
    
    def album_url(self, result_row: Tag) -> str:
        return result_row.find("div", {"class": "itemurl"}).attrs["href"]

    def n_tracks(self, result_row: Tag) -> int:
        result = result_row.find("div", {"class": "length"})
        return result.strip().split(" ")[0] if result else 0

    def title(self, result_row: Tag) -> str:
        result = result_row.find("div", {"class": "heading"})
        return result.text.strip() if result else ""

    def artist(self, result_row: Tag) -> str:
        result = result_row.find("div", {"class": "subhead"})
        return result.text.strip()[3:] if result else ""

    def year(self, result_row: Tag) -> str:
        result = result_row.find("div", {"class": "released"})
        return result.text.strip()[-4:] if result else ""

    def track_rows(self, album_pg: BeautifulSoup) -> List[Tag]:
        return album_pg.find_all("tr", {"class": "track_row_view"})

    def description(self, album_pg: BeautifulSoup) -> str:
        wrapper = album_pg.find("div", {"id", "trackInfo"})
        result = wrapper.find("div", {"class": "tralbumData"})
        return result.text if result else ""

    def tags(self, album_pg: BeautifulSoup) -> List[str]:
        wrapper = album_pg.find("div", {"id", "trackInfo"})
        result = wrapper.find_all("a", {"class": "tag"})
        return [_t.text for _t in result] if result else []

    def track_title(self, track_row: Tag) -> str:
        result = track_row.find("span", {"class": "track-title"})
        return result.text.strip() if result else ""

    def track_duration(self, track_row: Tag) -> int:
        result = track_row.find("span", {"class": "time"})
        duration_str = result.text.strip() if result else ""
        min_str, sec_str = duration_str.split(":")
        return str(int(min_str) * 60 + int(sec_str))

    def reviews(self, album_pg: BeautifulSoup) -> List[str]:
        return []  # TODO

    def parse_search_result(self, result_row) -> ParsedAlbum:
        return ParsedAlbum(
            self.title(result_row),
            artist=self.artist(result_row),
            year=self.year(result_row),
            api_url=self.album_url(result_row),
            n_tracks=self.n_tracks(result_row)
        )
    
    def parse_track_result(self, track_row: Tag) -> ParsedTrack:
        return ParsedTrack(
            self.track_title(track_row),
            duration=self.track_duration(track_row)
        )
    
    def parse_album_result(self, album_pg, album: ParsedAlbum) -> ParsedAlbum:
        album.tags = self.tags(album_pg)
        album.description = self.description(album_pg)
        album.reviews = self.reviews(album_pg)
        album.tracks = [self.parse_track_result(_t)
                        for _t in self.track_rows(album_pg)]


    #def populate_details(self, album_pg: BeautifulSoup):
    #    pass  # QUESTION: Does this realistically req a web driver? A: YES!



from interfaces.Interface import DatabaseInterface


SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS album (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  asset integer NOT NULL,
  name text NOT NULL,
  artist text NOT NULL,
  publisher text NOT NULL,
  year text NOT NULL,
  description text,
  site_url text NOT NULL
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
  album integer NOT NULL,
  tag integer NOT NULL,
  FOREIGN KEY (album)
    REFERENCES album (id)
    ON DELETE CASCADE,
  FOREIGN KEY (tag)
    REFERENCES tag (id)
    ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS review (
  id integer PRIMARY KEY,
  review_text text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS reviews (
  id integer PRIMARY KEY,
  album integer NOT NULL,
  review integer NOT NULL,
  FOREIGN KEY (album)
    REFERENCES album (id)
    ON DELETE CASCADE,
  FOREIGN KEY (review)
    REFERENCES review (id)
    ON DELETE CASCADE
);
"""
]

class Data(DatabaseInterface):

    def __init__(self, dbpath=""):
        super().__init__(dbpath)
        self.tag_cache = {}

    def record_result(self, catalog, asset_id, res: ParsedAlbum) -> bool:
        album_id = self.new_result(catalog, asset_id, res)
        if res.tags:
            for tag in res.tags:
                tag_id = self.get_cached_tag_id_with_insertion(tag)
                self.new_album_tag(album_id, tag_id)
        if res.reviews:
            for review in res.reviews:
                review_id = self.new_review(review)
                self.new_album_review(album_id, review_id)
        self.db.commit()
        return True

    def new_album(self, catalog, asset_id, res: ParsedAlbum) -> str:
        query = "INSERT INTO result (catalog, asset, name, site_id, size, downloads)"\
                "VALUES (?,?,?,?,?,?)"
        arguments = [catalog, asset_id, res.name, res.site_id, res.size, res.download_count]
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

    def new_album_tag(self, result_id, tag_id):
        self.c.execute("INSERT INTO tags (result, tag) VALUES (?,?)",
                       (result_id, tag_id))
        
    def new_review(self, review_text: str) -> str:
        self.c.execute("INSERT INTO review (review_text) VALUES (?)",
                       (review_text,))
        self.c.execute("SELECT last_insert_rowid();")
        return self.c.fetchone()[0]
        
    def new_album_review(self, result_id, review_id):
        self.c.execute("INSERT INTO reviews (result, review) VALUES (?,?)",
                       (result_id, review_id))
        