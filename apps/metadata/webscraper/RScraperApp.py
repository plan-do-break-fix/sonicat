


from apps.metadata.webscraper.ScraperApp import ScraperApp
from interfaces.scrapers.RuTracker import Data, WebScraper
from util.NameUtility import NameUtility as name

from typing import Dict

class WebScraperApp(ScraperApp):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, "RutrackerWebscraper")
        self.s = WebScraper()
        self.data = Data()
        self.wait = 10

    def search_by_label(self, catalog, label_id, pg=1) -> bool:
        label_res, asset_res, id_title_pairs = [], [], []
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
                    asset_res.append((catalog, _p[0], self.s.parse_result(_r)))
                continue
            if label_name.lower() in result_name:
                label_res.append((catalog, label_id, self.s.parse_result(_r)))
        if (pg - 1) < len(pages):
            next_res = self.search_by_label(catalog, label_id, pg=pg+1)
            label_res += next_res[0]
            asset_res += next_res[1]
        return (label_res, asset_res)



        