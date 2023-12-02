#!/usr/bin/python3

# Abstract base classes for application interfaces



import shutil
import sqlite3


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
        


import requests
from selenium import webdriver
import time

from user_agent import generate_user_agent
# for typing
from typing import Dict, Union
from requests import Response

THROTTLE_TIME = 4
USER_AGENT_TTL = 300
IP_ADDR_TTL = 300


class WebInterface:

    def __init__(self):
        self.useragent = generate_user_agent()
        self.login_url, self.session = False, False
        self.wait = THROTTLE_TIME
        self.t = {_k: time.time() for _k in ["throttle", "useragent", "ip"]}

    def login(self, username, password):
        if not self.login_url:
            print("WebInterface subclass must implement login_url attr to log in.")
            raise RuntimeError
        if not self.session:
            self.session = requests.Session()
        payload = {"username": username, "password": password}
        resp = self.post(self.login_url, payload)
        return bool(resp)

    def html_encode(self, query: str) -> str:
        query = query.replace(" - ", " ")
        return "%20".join(query.split(" "))
    
    def headers(self) -> Dict:
        return {"User-Agent": self.useragent}

    def cycle_useragent(self) -> bool:
        self.useragent = generate_user_agent()
        self.t["useragent"] = time.time()
        return True

    def cycle_ip(self) -> None:
        pass

    def throttle(self):
        if time.time() - self.t["throttle"] < self.wait:
            time.sleep(self.wait - (time.time() - self.t["throttle"]))
        self.t["throttle"] = time.time()
        return True

    def get_html_with_driver(url):
        driver = webdriver.Chrome()
        driver.get(url)
        source = driver.page_source
        driver.quit()
        return source

    def get(self, url: str, headers={}) -> requests.Response:
        headers=self.headers() if not headers else headers
        self.throttle()
        try:
            resp = requests.get(url, headers=self.headers(self.useragent))
        except ConnectionError:
            return e
        except requests.HTTPError as e:
            return e
        except requests.TooManyRedirects:
            return e
        except TimeoutError:
            return e
        return resp if resp.ok else self.handle_response(resp)

    def post(self, url: str, payload: Dict):
        if not self.session:
            return False
        headers=self.headers() if not headers else headers
        self.throttle()
        try:
            resp = self.session.post(url, data=payload, headers=headers)
        except ConnectionError:
            return e
        except requests.HTTPError as e:
            return e
        except requests.TooManyRedirects:
            return e
        except TimeoutError:
            return e
        return resp if resp.ok else self.handle_response(resp)

    def handle_response(self, resp: Response) -> Union[Response, bool]:
        if resp.status_code == 200:
            return resp
        elif 300 <= resp.status_code < 400:
            pass
        elif 400 <= resp.status_code < 500:
            pass
        elif 500 <= resp.status_code < 600:
            pass
        else:
            raise requests.HTTPError
        return False

    def retry(self, url):
        pass


    def domain_from_url(self, url: str) -> str:
        """
        Returns "<domain>.<tld>" from a URL. Returns empty string if input does
        not match pattern r"\w+\.\w+\/".
        """
        if url.startswith("http"):
            url = url.split("//")[1]
        if url.startswith("www"):
            url = url[4:]
        if url.endswith("/"):
            url = url[:-1]
        url = url.split("/")[0] if "/" in url else url
        return url if "." in url else ""
    
    def show_more():
        pass

