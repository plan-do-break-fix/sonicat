
from googlesearch import search
from interfaces.Interface import WebInterface
from typing import Dict, List

from util.NameUtility import Transform as names


class SearchInterface(WebInterface):

    def __init__(self):
        super().__init__()

    def product_search(self, cname: str, n=3) -> List:
        _res = search(cname, num_results=n, advanced=True)
        results = [_r for _r in _res]
        _, title, _ = names.divide_cname(cname)
        confirmed = [_r for _r in results
                     if (title.lower() in _r.title.lower()
                         or title.lower() in _r.description.lower())
                     ]
        return confirmed
