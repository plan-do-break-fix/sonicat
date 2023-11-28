
from bs4 import BeautifulSoup

#typing
from typing import List, Tuple
from bs4 import Tag

from interfaces.Interface import WebInterface


class WebScraper(WebInterface):

    def __init__(self):
        super().__init__()

    def page_soup(self, html: str) -> BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        return soup
    

