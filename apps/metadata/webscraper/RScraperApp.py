


from apps.metadata.webscraper.ScraperApp import ScraperApp
from interfaces.database.Catalog import CatalogInterface
from interfaces.scrapers.RuTracker import Data, WebScraper
from util.NameUtility import NameUtility as name

from typing import Dict

class WebScraperApp(ScraperApp):

    def __init__(self, sonicat_path: str, moniker: str) -> None:
        super().__init__(sonicat_path, moniker)
        self.s = WebScraper()
        self.data = Data()
        self.cat = {
            #"assets": CatalogInterface(f"{self.cfg.data}/catalog/AssetCatalog.sqlite"),
            "releases": CatalogInterface(f"{self.cfg.data}/catalog/ReleaseCatalog-ReadReplica.sqlite")
        } 

    def search_by_label(self, catalog, label_id, pg=1) -> bool:
        id_title_pairs = [], []
        label_name = self.cat[catalog].label(label_id)
        assets = self.cat[catalog].assets_by_label(label_id)
        if not all([label_name, assets]):
            return False
        url = self.s.query_url(label_name)
        soup = self.s.page_soup(self.s.get_content(url))
        if not soup:
            return False
        for _a in assets:
            if name.title_has_media_type_label(_a[1]):
                id_title_pairs.append((_a[0], name.drop_media_type_labels(_a[1])))
            else:
                id_title_pairs.append(_a)
        results = self.s.result_rows(soup)
        pages = self.s.pages(soup)
        for _r in results: 
            result_name = self.s.name(_r).lower()
            for _p in id_title_pairs:
                _tokens = _p[1].lower().split(" ")
                if all([_t in result_name for _t in _tokens]):
                    self.data.record_result(catalog, _p[0], _r)
        if (pg - 1) < len(pages):
            self.search_by_label(catalog, label_id, pg=pg+1)


        