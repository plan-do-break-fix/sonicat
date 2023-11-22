

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Union

import discogs_client
from discogs_client import Master

from interfaces.api.Client import ApiClient
from util.NameUtility import NameUtility

"""
API Reference: https://www.discogs.com/developers
API wrapper:   https://github.com/joalla/discogs_client

Ubiquitous Language
===
The result of a Discogs Client search is a list of Releases
Validating a search result confirms that the Discogs Release and Sonicat Cname
  reference the same entity
"""


@dataclass
class ParsedResultData:
    title: str
    discogsid: str
    year = ""
    country = ""
    cover_url = ""
    tags = []
    formats = []


class Client(ApiClient):

    def __init__(self, sonicat_path):
        secret = super().__init__(sonicat_path)
        self.conn = discogs_client.Client(secret["discogs"]["user_agent"],
                                        user_token=secret["discogs"]["token"])
        self.wait=2

    def search_releases(self, title, artist, year, master=True):
        type_str = "master" if master else "release"
        if artist and year:
            return self.conn.search(title, artist=artist, year=year, type=type_str)
        if artist:
            return self.conn.search(title, artist=artist, type=type_str)
        if year:
            return self.conn.search(title, year=year, type=type_str)
        return self.conn.search(title, type=type_str)

    def search(self, cname: str, track_durations: List[Decimal]) -> Union[Master, bool]:
        label, title, year = NameUtility.divide_cname(cname)
        if self.title_has_media_type_label(title):
            trimmed_title = self.drop_media_type_labels(title)
        else:
            trimmed_title = False
        # search by title, label as artist, year 
        results = self.safe_search(self.search_releases, [title, label, year])
        if not results and trimmed_title:
            results = self.safe_search(self.search_releases, [trimmed_title, label, year])
        release = self.validate_result(results, track_durations) if results else False
        if release:
            return release
        # search by title, label as artist
        results = self.safe_search(self.search_releases, [title, label, ""])
        if not results and trimmed_title:
            results = self.safe_search(self.search_releases, [trimmed_title, label, ""])
        release = self.validate_result(results, track_durations) if results else False
        if release:
            return release
        # search by title, year 
        results = self.safe_search(self.search_releases, [title, "", year])
        if not results and trimmed_title:
            results = self.safe_search(self.search_releases, [trimmed_title, "", year])
        release = self.validate_result(results, track_durations) if results else False
        if release:
            return release
        # search by concatenated label, title 
        search_str = f"{label} {title}"
        results = self.safe_search(self.search_releases, [search_str, "", ""])
        if not results and trimmed_title:
            search_str = f"{label} {trimmed_title}"
            results = self.safe_search(self.search_releases, [search_str, "", ""])
        release = self.validate_result(results, track_durations) if results else False
        if release:
            return release
        return False

    def validate_result(self, results: List[Master], track_durations: List[Decimal]) -> Master:
        if len(results) == 1:
            n_tracks_expected = len(track_durations)
            if (n_tracks_expected == len(results[0].tracklist)
                or 
                all([n_tracks_expected + 1 == len(results[0].tracklist),
                     int(results[0].tracklist[-1].duration.split(":")[0]) > 30,
                     "mix" in results[0].tracklist[0].title.lower()
                     ])
                ):
                expected_track_1_duration = track_durations[0]
                actual_track_1_duration = int(results[0].tracklist[-1].duration.split(":")[0]) * 60
                actual_track_1_duration += int(results[0].tracklist[-1].duration.split(":")[1])
                if actual_track_1_duration - 1 < expected_track_1_duration < actual_track_1_duration + 1:
                    return results[0]
            return False
        t_i = 0
        while t_i < max([len(results), 11]):
            release = self.validate_result([results[t_i]])
            if release:
                return release
            t_i += 1

    def parse_result(self, rawresult) -> ParsedResultData:
        prd = ParsedResultData(title=rawresult.data["title"],
                               discogsid=rawresult.data["id"]
                               )
        if "year" in rawresult.data.keys():
            prd.year += rawresult.data["year"]
        if "country" in rawresult.data.keys():
            prd.country = rawresult.data["country"]
        if "genre" in rawresult.data.keys():
            prd.tags += rawresult.data["genre"]
        if "style" in rawresult.data.keys():
            prd.tags += rawresult.data["style"]
        if "format" in rawresult.data.keys():
            prd.formats = rawresult.data["format"]
        if "cover_image" in rawresult.data.keys():
            prd.cover_url = rawresult.data["cover_image"]
        if prd.tags:
            prd.tags = list(set(prd.tags))
        if prd.formats:
            prd.formats = list(set(prd.formats))
        return prd

    
        
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

    def record_result(self, catalog, asset_id, res: ParsedResultData):
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

    def new_result(self, catalog, asset_id, res: ParsedResultData) -> str:
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
    
    def all_failed_searched_by_catalog(self, catalog):
        self.c.execute("SELECT asset FROM failedsearch;")
        return [_i[0] for _i in self.c.fetchall()]