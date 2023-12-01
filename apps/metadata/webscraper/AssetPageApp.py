

from googlesearch import SearchResult
from random import randint
from time import sleep
from typing import List

#from interfaces.Interface import DatabaseInterface
from apps.ConfiguredApp import App
from interfaces.Catalog import CatalogInterface
from interfaces.Search import SearchInterface, SearchResults
from util.Logs import initialize_logging



class SearchApp(App):

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        catalog_db_path = f"{self.cfg.data}/catalog/{self.cfg.name}.sqlite"
        self.catalog = CatalogInterface(catalog_db_path)
        self.cfg.log += "/pages"
        self.cfg.name = "PageSearch"
        self.log = initialize_logging(self.cfg)
        self.searches = SearchInterface()
        self.results = SearchResults(f"{self.cfg.data}/pages/PageSearch.sqlite")

    def unsearched_asset(self) -> str:
        searched_assets = self.results.all_searched_assets(self.cfg.app_key)
        searched_assets = [str(_i) for _i in searched_assets]
        return self.catalog.all_asset_ids(limit=1,
                                          excluding=searched_assets
                                          )[0]

    def search(self, asset_id: str) -> List[SearchResult]:
        cname = self.catalog.asset_cname(asset_id)
        self.log.info(f"Searching cname: {cname}")
        _results = self.searches.top_n_urls(cname)
        self.log.info(f"Recording {len(_results)} results")
        self.results.record_search(asset_id, self.cfg.app_key, _results)

    def ms_pause_duration(self) -> int:
        """Returns integer value: 10000 <= x <= 12500"""
        return 10000 + randint(0,2500)

    def run(self):
        asset_id = self.unsearched_asset()
        while asset_id:
            sleep(self.ms_pause_duration()/1000)
            self.search(asset_id)
            asset_id = self.unsearched_asset()




    


class ProductPageApp(SearchApp):

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)

    def domain_freq_dict(self):
        result = {}
        for _u in self.results.all_urls(self.cfg.app_key):
            domain = self.search.domain_from_url(_u)
            if domain not in result.keys():
                result[domain] = 0
            result[domain] += 1
        return result 