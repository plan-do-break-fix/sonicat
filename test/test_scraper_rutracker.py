
import pytest

from interfaces.scrapers.RuTracker import WebScraper
from test.test_data.rutracker import generator as g

@pytest.fixture
def scraper():
    return WebScraper()

RESULTS_PAGE_SOUP = g.results_page_soup()
RESULT_ROWS = g.result_rows()


def test_result_rows_length(scraper):
    result_rows = scraper.result_rows(RESULTS_PAGE_SOUP)
    assert len(result_rows) == 50

def test_pages(scraper):
    assert len(scraper.pages(RESULTS_PAGE_SOUP)) == 9

@pytest.mark.parametrize(
    "result_row, expected",
    [
        (RESULT_ROWS[0], "Locked Club - коллекция (16 релизов), 2018-2023, FLAC, (tracks), lossless"),
        (RESULT_ROWS[1], "Underworld - Born Slippy (TVT 8745-2) - 1996, FLAC (image+.cue), lossless")
    ]
)
def test_name(result_row, expected, scraper):
    assert expected == scraper.name(result_row)

@pytest.mark.parametrize(
    "result_row, expected",
    [
        (RESULT_ROWS[0], ["electro techno punk", "web"]),
        (RESULT_ROWS[1], ["techno", "drum n bass", "cd"])
    ]
)
def test_tags(result_row, expected, scraper):
    assert expected == [_t.lower() for _t in scraper.tags(result_row)]

@pytest.mark.parametrize(
    "result_row, expected",
    [
        (RESULT_ROWS[0], "47"),
        (RESULT_ROWS[1], "170")
    ]
)
def test_download_count(result_row, expected, scraper):
    assert expected == scraper.download_count(result_row)

@pytest.mark.parametrize(
    "result_row, expected",
    [
        (RESULT_ROWS[0], "6447330"),
        (RESULT_ROWS[1], "6410503")
    ]
)
def test_site_id(result_row, expected, scraper):
    assert expected == scraper.site_id(result_row)

@pytest.mark.parametrize(
    "result_row, expected",
    [
        (RESULT_ROWS[0], "1510"),
        (RESULT_ROWS[1], "370.6")
    ]
)
def test_size(result_row, expected, scraper):
    assert expected == scraper.size(result_row)

def test_parse_result():
    pass
