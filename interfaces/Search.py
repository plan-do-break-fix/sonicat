
from googlesearch import search, SearchResult
from time import sleep
from typing import Dict, List

from interfaces.Catalog import CatalogInterface
from interfaces.Interface import WebInterface
from util.NameUtility import Transform as names


class SearchInterface(WebInterface):

    def __init__(self):
        super().__init__()

    def top_n_urls(self, cname: str, n=5) -> List[SearchResult]:
        _res = search(cname, num_results=n, advanced=True)
        return [_r for _r in _res]



SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS asset_search_result (
    id integer PRIMARY KEY,
    asset integer NOT NULL,
    url text NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    FOREIGN KEY (asset) REFERENCES asset (id)
);
"""
]

class SearchResults(CatalogInterface):

    def __init__(self, dbpath=""):
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)

    def new_result(self, asset: int,
                         url: str,
                         title: str,
                         description: str,
                         finalize=False
                         ) -> bool:
        self.c.execute("""INSERT INTO asset_search_result
                          (asset, url, title, description) 
                          VALUES (?,?,?,?)""",
                          (asset, url, title, description))
        if finalize:
            self.db.commit()
        return True
        
    def n_results(self, asset: int) -> int:
        self.c.execute("""SELECT COUNT * FROM asset_search_result
                          WHERE asset = ?;""",
                          (asset,))
        result = self.c.fetchone()
        return result[0] if result else 0

    def asset_urls(self, asset: int) -> List[str]:
        pass

    def asset_results(self, asset: int) -> List[Dict]:
        pass

    def label_urls(self, label: int) -> List[str]:
        self.c.execute("""SELECT r.url FROM asset_search_result r
                          JOIN asset a ON r.asset = a.id
                          WHERE a.label = ?;""",
                          (label,))
        result = self.c.fetchall()
        return result if result else []
