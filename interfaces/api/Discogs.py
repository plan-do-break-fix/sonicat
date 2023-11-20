

from dataclasses import dataclass
from typing import List, Union

import discogs_client

from interfaces.api.Client import ApiClient
from util.NameUtility import NameUtility

@dataclass
class Result:
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

    def search_label_title(self, label: str, title: str, year: str):
        results = self.conn.search(f"{label} {title}")
        return results[0]

    def search_title_year(self, title: str, year: str):
        results = self.conn.search(f"{title} {year}")
        return results[0]

    def search(self, cname: str) -> Union[discogs_client.Release, bool]:
        label, title, year = NameUtility.divide_cname(cname)
        results = self.safe_search(self.search_label_title, [label, title])
        # // 
        result_lt = self.safe_search(self.search_label_title, [label, title])
        if result_lt == False and self.title_has_media_type_label(title):
            trimmed_title = self.drop_media_type_labels(title)
            result_lt = self.safe_search(self.search_label_title, [label, trimmed_title])
        if self.validate_result(cname, result_lt):
            return result_lt
        result_ty = self.safe_search(self.search_label_title, [title, year])
        if result_ty == False and self.title_has_media_type_label(title):
            trimmed_title = self.drop_media_type_labels(title)
            result_ty = self.safe_search(self.search_label_title, [trimmed_title, year])
        if any([self.validate_result(cname, result_ty),
                self.validate_result(cname, result_lt, result_ty)
                ]):
            return result_ty
        return False
    
    def exact_match(self, label, title, result) -> bool:
        return bool(result.data["title"].lower() == f"{label} - {title}".lower())
    
    def title_inclusion_with_year(self, title, year, result) -> bool:
        return all([result.data["year"] == year,
                    title.lower() in result.data["title"].lower()
                    ])
    
    def result_equality_with_year(self, year, result1, result2) -> bool:
        return all([result1 == result2,
                    result1.data["year"] == year
                    ])

    def validate_result(self, cname, result, result2=False) -> bool:
        label, title, year = NameUtility.divide_cname(cname)
        if self.title_has_media_type_label(title):
            title = self.drop_media_type_labels(title)
        if not result2:
            if self.safe_check(self.exact_match, [label, title, result]):
                return True
            if self.safe_check(self.title_inclusion_with_year, [title, year, result]):
                return True
            return False
        if self.safe_check(self.result_equality_with_year, [year, result, result2]):
            return True
        return False
        
    def process_result(self, rawresult) -> Result:
        res = Result(title=rawresult.data["title"],
                     discogsid=rawresult.data["id"]
                     )
        if "year" in rawresult.data.keys():
            res.year += rawresult.data["year"]
        if "country" in rawresult.data.keys():
            res.country = rawresult.data["country"]
        if "genre" in rawresult.data.keys():
            res.tags += rawresult.data["genre"]
        if "style" in rawresult.data.keys():
            res.tags += rawresult.data["style"]
        if "format" in rawresult.data.keys():
            res.formats = rawresult.data["format"]
        if "cover_image" in rawresult.data.keys():
            res.cover_url = rawresult.data["cover_image"]
        if res.tags:
            res.tags = list(set(res.tags))
        if res.formats:
            res.formats = list(set(res.formats))
        return res

    '''
    def OLD_search(self, cname: str):
        _label, _title, _year = NameUtility.divide_cname(cname)
        self.throttle()
        results1 = self.conn.search(f"{_label} {_title}")
        try:
            if results1[0].data["title"].lower() == f"{_label} - {_title}":
                return results1[0]
            elif all([
                      results1[0].data["year"] == _year,
                      _title.lower() in results1[0].data["title"].lower()
                      ]):
                return results1[0]
        except IndexError:
            pass
        except KeyError:
            pass
        self.throttle()
        results2 = self.conn.search(f"{_title} {_year}")
        try:
            if results1[0] == results2[0]:
                return results1[0]
        except IndexError:
            pass
        try:
            if all([
                results2[0].data["year"] == _year,
                _title.lower() in results2[0].data["title"].lower()
                ]):
                return results2[0]
        except IndexError:
            pass
        except KeyError:
            pass
        try:
            if _title.lower() in results1[0].data["title"].lower():
                return results1[0]
            elif _title.lower() in results2[0].data["title"].lower():
                return results2[0]
        except IndexError:
            pass
        except KeyError:
            pass
        return False
    '''

    
        
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

    def record_result(self, catalog, asset_id, res: Result):
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

    def new_result(self, catalog, asset_id, res: Result) -> str:
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