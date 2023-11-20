

from typing import List

from contextlib import closing
import re
import time
from yaml import load, SafeLoader

class ApiClient:

    def __init__(self, sonicat_path):
        with closing(open(f"{sonicat_path}/config/secrets.yaml", "r")) as _f:
            secret = load(_f.read(), SafeLoader)
        self.last_call = time.time()
        return secret

    def throttle(self):
        if time.time() - self.last_call < self.wait:
            time.sleep(self.wait)   #intentional; max_t b/w calls ~= 2t
        self.last_call = time.time()
        return True

    def title_has_media_type_label(self, title: str) -> bool:
        return any([_l in title for _l in [" CD", " EP", " LP"]])

    def drop_media_type_labels(self, title: str) -> str:
        PATTERNS = [
            r"\bM?CDM?S\d?\b",
            r"\b[EL]P\d?\b"
        ]
        for _p in PATTERNS:
            title = re.sub(_p, "", title)
        return title

    def safe_search(self, search_method: str, args: List[str]):
        self.throttle()
        try:
            return search_method(*args)
        except IndexError:
            return False

    def safe_check(self, method: str, args: List[str]):
        try:
            return method(*args)
        except IndexError:
            return False
        except KeyError:
            return False
