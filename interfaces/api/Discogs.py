#!/usr/bin/python3

import discogs_client
# Typing
from typing import List
from discogs_client import Release, Track

from interfaces.api.Client import ApiClient, ParsedAlbum, ParsedTrack

"""
API Reference: https://www.discogs.com/developers
API wrapper:   https://github.com/joalla/discogs_client

Ubiquitous Language
===
The result of a Discogs Client search is a list of Releases
Validating a search result confirms that the Discogs Release and Sonicat Cname
  reference the same entity
"""


class Client(ApiClient):

    def __init__(self, sonicat_path):
        secret = super().__init__(sonicat_path)
        self.conn = discogs_client.Client(secret["discogs"]["user_agent"],
                                        user_token=secret["discogs"]["token"])
        self.wait=2

    def search(self, title, artist="", publisher="", year="") -> bool:
        kwargs = {"type": "release"}
        if artist:
            kwargs["artist"] = artist
        if publisher:
            kwargs["label"] = publisher
        if year:
            kwargs["year"] = year
        self.throttle()
        results = self.conn.search(title, **kwargs)
        if results.count == 0:
            return False
        self.set_active_search(results.page(0))
        return True

    def next_result(self) -> Release:
        if not self.active_search:
            return False
        res = self.active_search[self.next_result_index]
        self.next_result_index += 1
        return res

    def tracks(self, rawresult: Release) -> List[ParsedTrack]:
        rawtracks = rawresult.tracklist
        tracks = [self.parse_track_result(_t) for _t in rawtracks]
        return tracks if all(tracks) else False

    def parse_album_result(self, rawresult: Release) -> ParsedAlbum:
        res = ParsedAlbum(title=rawresult.data["title"])
      # artist
        if "artists" in rawresult.data.keys() and len(rawresult["artists"]) == 1:
            res.artist = rawresult["artists"][0].data["name"]
      # catalog
        if "catno" in rawresult.data.keys():
            res.catalog = rawresult.data["catno"]
      # year
        if "year" in rawresult.data.keys():
            res.year += rawresult.data["year"]
      # album cover URL
        if "cover_image" in rawresult.data.keys():
            res.cover_url = rawresult.data["cover_image"]
      # tags
        if "genre" in rawresult.data.keys():
            res.tags += rawresult.data["genre"]
        if "style" in rawresult.data.keys():
            res.tags += rawresult.data["style"]
        if res.tags:
            res.tags = list(set(res.tags)) 
      # country
        if "country" in rawresult.data.keys():
            res.country = rawresult.data["country"]
      # formats
        if "format" in rawresult.data.keys():
            res.formats = rawresult.data["format"]
        if res.formats:
            res.formats = list(set(res.formats))
      # tracks
        _parsed_tracks = self.tracks(rawresult)
        res.tracks = _parsed_tracks if _parsed_tracks else []
      # API identifiers
        res.api_id = rawresult.data["id"]
        res.api_url = rawresult.data["resource_url"]
        return res

    def parse_track_result(self, rawresult: Track) -> ParsedTrack:
        if not rawresult.title:
            return False
        res = ParsedTrack(title=rawresult.title)
      # artist
        if rawresult.artists and len(rawresult.artists) == 1:
            res.artist = rawresult.artists[0]
      # duration
        if "duration" not in rawresult.data.keys():
            res.duration = 0
        min_str, sec_str = rawresult.data["duration"].split(":")
        res.duration = int(min_str) * 60 + int(sec_str)
      #
        return res
        
from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS result (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  asset integer NOT NULL,
  title text NOT NULL,
  year integer NOT NULL,
  discogsid integer NOT NULL,
  country text,
  cover_url text
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
CREATE TABLE IF NOT EXISTS resulttags (
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
""",
"""
CREATE TABLE IF NOT EXISTS format (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS resultformats (
  id integer PRIMARY KEY,
  result integer NOT NULL,
  format integer NOT NULL,
  FOREIGN KEY (result)
    REFERENCES result (id)
    ON DELETE CASCADE,
  FOREIGN KEY (format)
    REFERENCES format (id)
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
        self.format_cache = {}

    def record_result(self, catalog, asset_id, res: ParsedAlbum):
        result_id = self.new_result(catalog, asset_id, res)
        if res.tags:
            for tag in res.tags:
                tag_id = self.get_cached_tag_id_with_insertion(tag)
                self.new_result_tag(result_id, tag_id)
        if res.formats:
            for format in res.formats:
                format_id = self.get_cached_format_id_with_insertion(format)
                self.new_result_format(result_id, format_id)
        return True

    def record_failed_search(self, catalog, asset_id):
        self.c.execute("INSERT INTO failedsearch (catalog, asset) VALUES (?,?);",
                       (catalog, asset_id))

    def drop_failed_search(self, catalog, asset_id):
        self.c.execute("DELETE FROM failedsearch WHERE catalog = ? AND asset = ?;",
                       (catalog, asset_id))
        self.db.commit()

    def new_result(self, catalog, asset_id, res: ParsedAlbum) -> str:
        query = "INSERT INTO result (catalog, asset, title, year, discogsid"
        arguments = [catalog, asset_id, res.title, res.year, res.discogsid]
        if res.country:
            query += ", country"
            arguments.append(res.country)
        if res.cover_url:
            query += ", cover_url"
            arguments.append(res.cover_url)
        query += f") VALUES (?{',?' * (len(arguments)-1)});"
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
    
    def get_cached_format_id_with_insertion(self, format: str) -> str:
        format = format.lower()
        if format not in self.format_cache:
            self.c.execute('SELECT id FROM format WHERE name = ?;', (format,))
            result = self.c.fetchone()
            if not result:
                self.c.execute("INSERT INTO format (name) VALUES (?);", (format,))
                self.c.execute("SELECT last_insert_rowid();")
                result = self.c.fetchone()
            self.format_cache[format] = result[0]
        return self.format_cache[format]
    
    def new_result_tag(self, result_id, tag_id):
        self.c.execute("INSERT INTO resulttags (result, tag) VALUES (?,?)",
                       (result_id, tag_id))
        return True
    
    def new_result_format(self, result_id, format_id):
        self.c.execute("INSERT INTO resultformats (result, format) VALUES (?,?)",
                       (result_id, format_id))
        return True
    
    def all_asset_ids_by_catalog(self, catalog):
        self.c.execute("SELECT asset FROM result WHERE catalog = ?;", (catalog,))
        return [_i[0] for _i in self.c.fetchall()]
    
    def all_failed_searches_by_catalog(self, catalog):
        self.c.execute("SELECT asset FROM failedsearch WHERE catalog=?;", (catalog,))
        return [_i[0] for _i in self.c.fetchall()]

    def all_results_by_asset(self, asset_id, catalog):
        self.c.execute("SELECT * FROM results WHERE asset = ? AND catalog = ?",
                       (asset_id, catalog))
        return self.c.fetchall()