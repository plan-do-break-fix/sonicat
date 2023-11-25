#!/usr/bin/python3

# Abstract base classes for application interfaces



from bs4 import BeautifulSoup
import requests
import shutil
import sqlite3
from typing import List

from util.NameUtility import NameUtility

class DatabaseInterface:

    def __init__(self, dbpath=""):
        self.dbpath = dbpath
        self.db = sqlite3.connect(dbpath)
        self.c = self.db.cursor()

    def commit(self):
        return self.db.commit()

    def export_replica(self, note=""):
        self.db.commit()
        self.db.close()
        replica_path = self.dbpath.replace(".sqlite", f"-ReadReplica{note}.sqlite")
        shutil.copyfile(self.dbpath,replica_path)
        self.db = sqlite3.connect(self.dbpath)
        self.c = self.db.cursor()
        



class WebInterface:

    def __init__(self):
        pass
        #self.names = NameUtility.Transform()

    def encode(self, query: str) -> str:
        query = query.replace(" - ", " ")
        return "%20".join(query.split(" "))

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
        return False

    def domain_from_url(self, url: str) -> str:
        if url.startswith("http"):
            url = url.split("//")[1]
        if url.startswith("www"):
            url = url[4:]
        if url.endswith("/"):
            url = url[:-1]
        if not "/" in url:
            return url
        else:
            return url.split("/")[0]
    
    def description_by_tag_attrs(self, soup: BeautifulSoup) -> List[str]:
        body, description = soup.find("body"), []
        tags = ["div", "p"]
        attributes = ["class", "id", "itemprop"]
        values = ["desc", "copy-content"]
        result_lists = [body.find_all(_tag) for _tag in tags]
        all_results = [_r for _rl in result_lists for _r in _rl]
        for result in all_results:
            for _attr in attributes:
                if _attr not in result.attrs.keys():
                    continue
                for _val in values:
                    if _val in result.attrs[_attr]:
                        description.append(result.text)
        return description


    def contents_from_description(self, soup: BeautifulSoup) -> List[str]:
        pass

    def content_section_tag(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        pass

    def contents_from_tag(self, tag: BeautifulSoup) -> List[str]:
        pass

    def show_more():
        pass

    def preview_images_by_alt_text(self, cname: str,
                                         soup: BeautifulSoup
                                         ) -> List[str]:
        _, product, _ = NameUtility.divide_cname(cname)
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

    #def preview_images_by_fname(self, cname: str,
    #                                  soup: BeautifulSoup
    #                                  ) -> List[str]:
    #    _, product, _ = self.names.divide_cname(cname)
    #    candidates = []
    #    if " - " in product:
    #        prod_names = product.split(" - ")
    #    else:
    #        prod_names = []
    #    prod_names.append(product)
    #    name_forms = [_form for _list in
    #                  self.names.name_forms()
    #                  for _form in _list]
    #    all_imgs = soup.find_all("img")
    #    for _img in all_imgs:
    #        for _form in name_forms:
    #            if _form in _img.attrs.src.split("/")[-1]:
    #                candidates.append(_img)
    #    return candidates


