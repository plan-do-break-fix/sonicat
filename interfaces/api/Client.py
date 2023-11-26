


from contextlib import closing
from dataclasses import dataclass
import re
import time
from yaml import load, SafeLoader

from typing import List


@dataclass
class ParsedAlbum:
    title: str
    artist = ""
    publisher = ""
    catalog = ""
    year = ""
    cover_url = ""
    tags = []
    description = ""
    country = ""
    formats = []
    tracks = []
    api_id = ""
    api_url = ""
    listener_count = ""
    play_count = ""

    def track_durations(self) -> List[int]:
        return [_t.duration for _t in self.tracks]

@dataclass
class ParsedTrack:
    title: str
    artist = ""
    duration = ""
    tags = []
    description = ""
    api_id = ""
    api_url = ""
    listener_count = ""
    play_count = ""



class ApiClient:

    def __init__(self, sonicat_path):
        self.active_search = None
        self.next_result_index = 0
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
            r"\b(MCD|CD(M|M?S|R))\d?\b",
            r"\b[EL]P\d?\b"
        ]
        for _p in PATTERNS:
            title = re.sub(_p, "", title)
        return title

    def safe_check(self, method: str, args: List[str]):
        try:
            return method(*args)
        except IndexError:
            return False
        except KeyError:
            return False

    def set_active_search(self, search_results_iterable) -> bool:
        self.active_search = search_results_iterable
        self.next_result_index = 0
        return True

    def validate_by_track_durations(self, measured, result, th=2.0) -> bool:
        if any([
                len(measured) != len(result),
                sum(result) == 0
                ]):
            return False
        for _i, _duration in enumerate(measured):
            if not result[_i] - th <= _duration <= result[_i] + th:
                return False
        return True