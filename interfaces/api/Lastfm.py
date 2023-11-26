#!/usr/bin/python3

import pylast
import re
# Typing
from typing import List, Tuple
from pylast import Album, Track

from interfaces.api.Client import ApiClient, ParsedAlbum, ParsedTrack

IGNORE_TAGS = [
    f".*\bown\b.*",
    f".*\blisten\b.*",
    f".*\byou (must|should|have)\b.*",
    f"favorites?"
]

class Client(ApiClient):

    def __init__(self, sonicat_path):
        secret = super().__init__(sonicat_path)
        pylast.HEADERS["User-Agent"] = secret["lastfm"]["user_agent"]
        self.conn = pylast.LastFMNetwork(
            api_key=secret["lastfm"]["api_key"],
            api_secret=secret["lastfm"]["shared_secret"]
            )
        self.wait = 1

    def search(self, album: str) -> bool:
        result = self.conn.search_for_album(album)
        if not result:
            return False
        page = result.get_next_page()
        self.set_active_search(page)

    def next_result(self) -> Album:
        if not self.active_search:
            return False
        res = self.active_search[self.next_result_index]
        self.next_result_index += 1
        return res

    def tracks(self, rawresult: Album) -> List[ParsedTrack]:
        rawtracks = rawresult.get_tracks()
        tracks = [self.parse_track_result(_t) for _t in rawtracks]
        return tracks if all(tracks) else False

    def parse_album_result(self, rawresult: Album) -> ParsedAlbum:
        _title = rawresult.get_title()
        if not _title:
            return False
        res = ParsedAlbum(title=_title)
      # artist
        _artist = rawresult.get_artist()
        res.artist = _artist.get_name() if _artist else ""
      # cover image URL
        _cover = rawresult.get_cover_image()
        res.cover_url = _cover if _cover else ""
      # tags, year
        _tags = rawresult.get_top_tags()
        res.tags = [_t.item.name for _t in _tags] if _tags else []
      # description
        _description = rawresult.get_wiki_summary()
        res.description = _description if _description else ""
      # tracks
        _parsed_tracks = self.tracks(rawresult)
        res.tracks = _parsed_tracks if _parsed_tracks else []
      # API identifiers
        _url = rawresult.get_url()
        res.api_url = _url if _url else ""
      # counts
        _count = rawresult.get_listener_count()
        res.listener_count = str(_count) if _count else ""
        _count = rawresult.get_playcount()
        res.play_count = str(_count) if _count else ""
      #
        return res

    def parse_track_result(self, rawresult: Track) -> ParsedTrack:
        _title = rawresult.get_title()
        if not _title:
            return False
        res = ParsedTrack(title=_title)
      # artist
        _artist = rawresult.get_artist()
        res.artist = _artist.get_name() if _artist else ""
      # duration
        _duration = rawresult.get_duration()
        res.duration = int(_duration/1000) if _duration else 0
      # tags
        _tags = rawresult.get_top_tags()
        res.tags = [_t.item.name for _t in _tags] if _tags else []
      # description
        _description = rawresult.get_wiki_summary()
        res.description = _description if _description else ""
      # API identifiers
        _url = rawresult.get_url()
        res.api_url = _url if _url else ""
      # counts
        _count = rawresult.get_listener_count()
        res.listener_count = str(_count) if _count else ""
        _count = rawresult.get_playcount()
        res.play_count = str(_count) if _count else ""
      #
        return res


from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS albumresult (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  asset integer NOT NULL,
  title text NOT NULL,
  year integer NOT NULL,
  country text,
  cover_url text
);
""",
"""
CREATE TABLE IF NOT EXISTS trackresult (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  file integer NOT NULL,
  title text NOT NULL,
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
CREATE TABLE IF NOT EXISTS albumtags (
  id integer PRIMARY KEY,
  albumresult integer NOT NULL,
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
CREATE TABLE IF NOT EXISTS tracktags (
  id integer PRIMARY KEY,
  trackresult integer NOT NULL,
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
        for _query in SCHEMA:
            self.c.execute(_query)
        self.db.commit()
        self.tag_cache = {}

    def record_album_result(self, catalog, asset_id, res: ParsedAlbum, finalize=False) -> bool:
        result_id = self.new_album_result(catalog, asset_id, res)
        if res.tags:
            for tag in res.tags:
                tag_id = self.get_cached_tag_id_with_insertion(tag)
                self.new_album_result_tag(result_id, tag_id)
        if finalize:
            self.db.commit()
        return True

    def record_track_result(self, catalog, file_id, res: TrackResult, finalize=False) -> bool:
        result_id = self.new_track_result(catalog, file_id, res)
        if res.tags:
            for tag in res.tags:
                tag_id = self.get_cached_tag_id_with_insertion(tag)
                self.new_track_result_tag(result_id, tag_id)
        if finalize:
            self.db.commit()
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
    
    def new_album_result(self, catalog, asset_id, res: ParsedAlbum) -> str:
        query = "INSERT INTO albumresult (catalog, asset, title, year"
        arguments = [catalog, asset_id, res.title, res.year]
        if res.cover_url:
            query += ", cover_url"
            arguments.append(res.cover_url)
        query += f") VALUES (?{',?' * (len(arguments)-1)});"
        self.c.execute(query, arguments)
        self.c.execute("SELECT last_insert_rowid();")
        return self.c.fetchone()[0]

    def new_track_result(self, catalog, file_id, res: TrackResult) -> str:
        query = "INSERT INTO trackresult (catalog, file, title"
        arguments = [catalog, file_id, res.title]
        if res.lyrics:
            query += ", lyrics"
            arguments.append(res.lyrics)
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
    
    def new_album_result_tag(self, album_result_id, tag_id) -> bool:
        self.c.execute("INSERT INTO albumtags (result, tag) VALUES (?,?)",
                       (album_result_id, tag_id))
        return True

    def new_track_result_tag(self, track_result_id, tag_id) -> bool:
        self.c.execute("INSERT INTO tracktags (result, tag) VALUES (?,?)",
                       (track_result_id, tag_id))
        return True

    def all_asset_ids_by_catalog(self, catalog) -> List[str]:
        self.c.execute("SELECT asset FROM result WHERE catalog = ?;",
                       (catalog,))
        return [_i[0] for _i in self.c.fetchall()]
    
    def all_failed_searched_by_catalog(self, catalog) -> List[str]:
        self.c.execute("SELECT asset FROM failedsearch WHERE catalog = ?;",
                       (catalog,))
        return [_i[0] for _i in self.c.fetchall()]