
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
    tracks = []
    api_id = ""

    def track_durations(self) -> List[int]:
        return [_t.duration for _t in self.tracks]

@dataclass
class ParsedTrack:
    title: str
    artist = ""
    duration = ""
    tempo = ""


class WebScraper(Scraper):

    def __init__(self):
        super().__init__()

    def query_url(self, query_text, format="flac") -> str:
        query = self.html_encode(f"{query_text} {format}")
        return f"https://www.junodownload.com/search/?q%5Ball%5D%5B%5D={query}"

    def result_rows(self, soup: BeautifulSoup) -> List[Tag]:
        rows = soup.find("div", {"id": "jd-listing-item"})
        return rows
    
    def site_id(self, result_row: Tag) -> str:
        href = result_row.find("a", {"class": "juno-title"}).attrs["href"]
        href = href[:-1] if href.endswith("/") else href
        return href.split("/")[-1]

    def title(self, result_row: Tag) -> str:
        return result_row.find("a", {"class": "juno-title"}).text

    def artist(self, result_row: Tag) -> str:
        return result_row.find("a", {"class": "juno-artist"}).text

    def publisher(self, result_row: Tag) -> str:
        return result_row.find("a", {"class": "juno-label"}).text

    def catalog_year_genre(self, result_row: Tag) -> str:
        lines = result_row.find("div", {"class": "text-muted"}).text.split("\n")
        yr_int = int(lines[1].split(" ")[-1])
        year = f"20{str(yr_int)}" if yr_int < 50 else f"19{yr_int}"
        return (lines[0], year, lines[2])
    
    def tracks(self, result_row: Tag) -> List:
        wrapper = result_row.find("div", {"class": "jd-listing-tracklist"})
        return wrapper.find_all("div", {"class": "row"})

    def track_title_artist_duration_tempo(self, track_row: Tag) -> Tuple[str]:
        text = track_row.find("div", {"class": "jq_highlight"})
        title_str, the_rest = text.split(" - (")
        duration_str, the_rest = the_rest.split(") ")
        tempo = the_rest.split(" ")[0]
        if "- \"" in title_str:
            artist, title = title_str.split(" - \"")
            title = title.replace("\"", "")
        else:
            artist, title = "", title_str
        min_str, sec_str = duration_str.split(":")
        duration = str(int(min_str) * 60 + int(sec_str))
        return (title, artist, duration, tempo)

    def parse_album_result(self, result_row: Tag) -> ParsedAlbum:
        tracks = [self.parse_track_row(_r) for _r in self.tracks(result_row)]
        catalog, year, genre = self.catalog_year_genre(result_row)
        return ParsedAlbum(
            title=self.title(result_row),
            artist=self.artist(result_row),
            catalog=catalog,
            year=year,
            genre=genre,
            tracks=tracks,
            api_id=self.site_id(result_row)
        )
    
    def parse_track_row(self, track_row: Tag) -> ParsedTrack:
        title, artist, duration, tempo = self.track_title_artist_duration_tempo(track_row)
        return ParsedTrack(
                           title=title,
                           artist=artist,
                           duration=duration,
                           tempo=tempo
                           )


from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS albumresult (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  asset int NOT NULL,
  title text NOT NULL,
  artist text NOT NULL,
  publisher text NOT NULL,
  genre integer,
  year text,
  catalog text,
  FOREIGN KEY (genre)
    REFERENCES genre (id)
    ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS trackresult(
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  file int NOT NULL,
  title text NOT NULL,
  artist text,
  duration text,
  tempo text
);
""",
"""
CREATE TABLE IF NOT EXISTS artist (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS publisher (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS failedsearch (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  asset integer NOT NULL
)
""",
"""
CREATE TABLE IF NOT EXISTS tag (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS albumtags (
  id integer PRIMARY KEY,
  albumresult integer NOT NULL,
  tag integer NOT NULL,
  FOREIGN KEY (albumresult)
    REFERENCES albumresult (id)
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
        for _query in SCHEMA:
            self.c.execute(_query)
        self.db.commit()
        self.tag_cache = {}
        self.artist_cache = {}
        self.publisher_cache = {}

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
    
    def get_cached_artist_id_with_insertion(self, artist) -> str:
        artist = artist.lower()
        if artist not in self.artist_cache:
            self.c.execute('SELECT id FROM artist WHERE name = ?;', (artist,))
            result = self.c.fetchone()
            if not result:
                self.c.execute("INSERT INTO artist (name) VALUES (?);", (artist,))
                self.c.execute("SELECT last_insert_rowid();")
                result = self.c.fetchone()
            self.artist_cache[artist] = result[0]
        return self.artist_cache[artist]
    
    def get_cached_publisher_id_with_insertion(self, publisher) -> str:
        publisher = publisher.lower()
        if publisher not in self.publisher_cache:
            self.c.execute('SELECT id FROM publisher WHERE name = ?;', (publisher,))
            result = self.c.fetchone()
            if not result:
                self.c.execute("INSERT INTO publisher (name) VALUES (?);", (publisher,))
                self.c.execute("SELECT last_insert_rowid();")
                result = self.c.fetchone()
            self.publisher_cache[publisher] = result[0]
        return self.publisher_cache[publisher]

    def record_result(self, catalog, asset_id, file_ids, res: ParsedAlbum):
        result_id = self.new_album_result(catalog, asset_id, res)
        if res.genre:
            tag_id = self.get_cached_tag_id_with_insertion(res.genre)
            self.new_album_tag(result_id, tag_id)
        if res.tracks and len(res.tracks) == len(file_ids):
            for _i, _t in enumerate(res.tracks):
                file_id = file_ids[_i]
                result_id = self.new_track_result(catalog, file_id, _t)
        return True

    def record_failed_search(self, catalog, asset_id) -> bool:
        self.c.execute("INSERT INTO failedsearch (catalog, asset) VALUES (?,?);",
                       (catalog, asset_id))
        self.db.commit()
        return True

    def drop_failed_search(self, catalog, asset_id) -> bool:
        self.c.execute("DELETE FROM failedsearch WHERE catalog = ? AND asset = ?;",
                       (catalog, asset_id))
        self.db.commit()
        return True
