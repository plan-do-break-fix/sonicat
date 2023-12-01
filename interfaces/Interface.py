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
import time

from user_agent import generate_user_agent

from typing import Dict

THROTTLE_TIME = 4
USER_AGENT_TTL = 300
IP_ADDR_TTL = 300


class WebInterface:

    def __init__(self):
        self.useragent = generate_user_agent()
        self.wait = THROTTLE_TIME
        self.t = {_k: time.time() for _k in ["throttle", "useragent", "ip"]}

    def html_encode(self, query: str) -> str:
        query = query.replace(" - ", " ")
        return "%20".join(query.split(" "))
    
    def headers(self, useragent: str) -> Dict:
        return {"User-Agent": useragent}

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

    def get_content(self, url: str) -> str:
        self.throttle()
        resp = requests.get(url, headers=self.headers(self.useragent))
        try:
            if resp.status_code == 200:
                return resp.content
            elif 300 <= resp.status_code < 400:
                pass
            elif 400 <= resp.status_code < 500:
                pass
            elif 500 <= resp.status_code < 600:
                pass
            else:
                raise requests.HTTPError
        except ConnectionError:
            return e
        except requests.HTTPError as e:
            return e
        except requests.TooManyRedirects:
            return e
        except TimeoutError:
            return e
        finally:
            print("How did you get here?")
            raise RuntimeError


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

