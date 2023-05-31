#!/usr/bin/python3

# Abstract base classes for external system interfaces
# JDSwan 2023.05.20



from bs4 import BeautifulSoup
import requests
import sqlite3
from typing import List

from util import NameUtility

class DatabaseInterface:

    def __init__(self, dbpath=""):
        self.db = sqlite3.connect(dbpath)
        self.c = self.db.cursor()

    def commit(self):
        return self.db.commit()


class WebInterface:

    def __init__(self):
        self.names = NameUtility.Transform()
        self.domains = {
            "social_platforms": [
                "facebook.com",
                "instagram.com",
                "linkedin.com",
                "tiktok.com",
                "twitter.com",
                "youtube.com"
            ]
        }

    def get_html(self, url: str) -> str:
        resp = requests.get(url)
        try:
            if resp.status_code == 200:
                return resp.content
            else:
                pass
        except:
            pass
        finally:
            pass

    def domain_from_url(self, url: str) -> str:
        if url.startswith("http"):
            url = url.split("//")[1]
        if url.startswith("www"):
            url = url[4:]
        if url.endswith("/"):
            url = url.split("/")[0]
        return url
    
    def parse_description(self, soup: BeautifulSoup) -> str:
        body, candidates = soup.find("body"), []
        tags = ["div", "p"]
        attributes = ["class", "id", "itemprop"]
        values = ["desc", "copy-content"]
        for _tag in tags:
            all_results = body.find_all(_tag)
            for result in all_results:
                for _attr in attributes:
                    if _attr in result.attrs.keys():
                        for _val in values:
                            if _val in result.attrs[_attr]:
                                candidates.append(result.text)
        return candidates

    def show_more():
        pass

    def preview_images_by_alt_text(self, cname: str,
                                         soup: BeautifulSoup
                                         ) -> List[str]:
        _, product, _ = self.names.divide_cname(cname)
        body, candidates = soup.find("body"), []
        if " - " in product:
            prod_names = product.split(" - ")
        else:
            prod_names = []
        imgs_w_alt = [_i for _i in body.find_all("img")
                      if "alt" in _i.attrs.keys()
                      ]
        for _img in imgs_w_alt:
            if product == _img.attrs.alt:
                candidates.append(_img)
                continue
            for _name in prod_names:
                if _name in _img.attrs.alt:
                    candidates.append(_img)
        return candidates

    def preview_images_by_fname(self, cname: str,
                                      soup: BeautifulSoup
                                      ) -> List[str]:
        _, product, _ = self.names.divide_cname(cname)
        candidates = []
        if " - " in product:
            prod_names = product.split(" - ")
        else:
            prod_names = []
        prod_names.append(product)
        name_forms = [_form for _list in
                      self.names.name_forms()
                      for _form in _list]
        all_imgs = soup.find_all("img")
        for _img in all_imgs:
            for _form in name_forms:
                if _form in _img.attrs.src.split("/")[-1]:
                    candidates.append(_img)
        return candidates


