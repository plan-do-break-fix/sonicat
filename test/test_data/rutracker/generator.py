
from bs4 import BeautifulSoup

RESULT_ROWS = [
"""
<tr id="trs-tr-6447330" class="tCenter hl-tr" data-topic_id="6447330" role="row">
<td id="6447330" class="row1 t-ico">
<img src="https://static.rutracker.cc/templates/v1/images/icon_minipost_new.gif" class="icon1" alt="N">
</td>
<td class="row1 t-ico" title="не проверено"><span class="tor-icon tor-not-approved">*</span></td>
<td class="row1 f-name-col">
<div class="f-name"><a class="gen f ts-text" href="https://rutracker.org/forum/tracker.php?f=1825&amp;nm=locked+club+flac">Techno (lossless)</a></div>
</td>
<td class="row4 med tLeft t-title-col tt">
<div class="wbr t-title">
<a data-topic_id="6447330" class="med tLink tt-text ts-text hl-tags bold tags-initialized" href="viewtopic.php?t=6447330">Locked Club - коллекция <span class="brackets-pair">(16 релизов)</span>, 2018-2023, FLAC, <span class="brackets-pair">(tracks)</span>, lossless</a>
</div>
<div id="tg-6447330" class="t-tags"><span class="tg">Electro Techno Punk</span><span class="tg">WEB</span></div>
</td>
<td class="row1 u-name-col">
<div class="wbr u-name"><a class="med ts-text" href="tracker.php?pid=36159980">wvaac</a></div>
</td>
<td class="row4 small nowrap tor-size" data-ts_text="1626378719">
<a class="small tr-dl dl-stub" href="dl.php?t=6447330">1.51&nbsp;GB ↓</a> </td>
<td class="row4 nowrap" data-ts_text="2">
<b class="seedmed">2</b> </td>
<td class="row4 leechmed bold" title="Личи">0</td>
<td class="row4 small number-format">47</td>
<td class="row4 small nowrap" style="padding: 1px 3px 2px;" data-ts_text="1700977309">
<p>26-Ноя-23</p>
</td>
</tr>
""",
"""
<tr id="trs-tr-6410503" class="tCenter hl-tr" data-topic_id="6410503" role="row">
<td id="6410503" class="row1 t-ico">
<img src="https://static.rutracker.cc/templates/v1/images/icon_minipost.gif" class="icon1" alt="o">
</td>
<td class="row1 t-ico" title="проверено"><span class="tor-icon tor-approved">√</span></td>
<td class="row1 f-name-col">
<div class="f-name"><a class="gen f ts-text" href="https://rutracker.org/forum/tracker.php?f=1825&amp;nm=born+slippy+flac">Techno (lossless)</a></div>
</td>
<td class="row4 med tLeft t-title-col tt">
<div class="wbr t-title">
<a data-topic_id="6410503" class="med tLink tt-text ts-text hl-tags bold tags-initialized" href="viewtopic.php?t=6410503">Underworld - Born Slippy <span class="brackets-pair">(TVT 8745-2)</span> - 1996, FLAC <span class="brackets-pair">(image+.<wbr>cue)</span>, lossless</a>
</div>
<div id="tg-6410503" class="t-tags"><span class="tg">Techno</span><span class="tg">Drum n Bass</span><span class="tg">CD</span></div>
</td>
<td class="row1 u-name-col">
<div class="wbr u-name"><a class="med ts-text" href="tracker.php?pid=86138">veramaxx</a></div>
</td>
<td class="row4 small nowrap tor-size" data-ts_text="388591610">
<a class="small tr-dl dl-stub" href="dl.php?t=6410503">370.6&nbsp;MB ↓</a> </td>
<td class="row4 nowrap" data-ts_text="9">
<b class="seedmed">9</b> </td>
<td class="row4 leechmed bold" title="Личи">1</td>
<td class="row4 small number-format">170</td>
<td class="row4 small nowrap" style="padding: 1px 3px 2px;" data-ts_text="1695021220">
<p>18-Сен-23</p>
</td>
</tr>
"""
]


def result_rows() -> BeautifulSoup:
    return [BeautifulSoup(html) for html in RESULT_ROWS]

def results_page_soup() -> BeautifulSoup:
    with open("test/test_data/rutracker/rutracker-results_page.html", "r", encoding="utf-8", errors="ignore") as _f:
        return BeautifulSoup(_f.read())