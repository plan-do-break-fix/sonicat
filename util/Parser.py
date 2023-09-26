
from dataclasses import dataclass
import re
from typing import List, Tuple

from util.NameUtility import NameUtility

REGEX_VERSION = r"(?<=v)(\d+\.)+(?=\b)"
REGEX_RAW_KEY = r"(?<=\b)[a-g] ?(b|#|sharp|flat)? ?(m(in|aj)?)?(or)?([2-7])?(?=(\s|\.|$))"
REGEX_RAW_TEMPO_POSTFIX = r"(\d{2,3}( )?bpm)"                           # matches common formats of labelled tempos
REGEX_RAW_TEMPO_PREFIX = r"(bpm ?\d{2,3}(?=\b))"                        # matches inverse formats of labelled tempos
REGEX_RAW_TEMPO_NO_LABEL = r"(?<=\b)(\d{2,3})(?=\b)"
RANGE_TEMPO_1 = (80, 140)
RANGE_TEMPO_2 = (60, 180)
RANGE_TEMPO_3 = (40, 240)
RANGE_TEMPO_4 = (20, 300)
SPACE_ALTS = [
    "/",
    "_",
    "-", "‒", "–", "—", "−",
    "~",
    "=",
    ",",
    ".",
    ":",
    "(",")",
    "[","]",
    "{","}",
    "<",">"
]
DROP_CHARS = [
    "'", "\"", "!", "?"
]

SUBSTITUTIONS = {
    "drumkit": ["drum kit"],
    "loopkit": ["loop kit"],
    "hihat": ["hi hat", "hh"],
    "hiphop": ["hip hop"],
    "lofi": ["lo fi"],
    "oneshot": ["one shot"]
}
KEY_SUBSTITUTIONS = {
    "sharp": "#",
    "flat": "b",
    "or": "",
    r"m($|[2-7])": "min"
}

@dataclass
class ParsedAudioFilePath:
    path: str
    key: str
    tempo: str
    tokens: List[str]


class AudioFilePathParser:

    def __init__(self) -> None:
        self.audio_exts = ["aif", "aiff", "flac", "mid", "midi", "mp3", "ogg", "wav"]
        self.cache = {}
        self.acronyms = []

    def parse_path(self, path: str) -> ParsedAudioFilePath:
        path = self.trim(path.lower())
        space_normal_path = self.normal_spaces(path)
        raw_tempo, tempo = self.parse_tempo(space_normal_path)
        raw_key, key_sig = self.parse_key_signature(space_normal_path)
        stripped_path = path.replace(raw_tempo, "").replace(raw_key, "")
        normal_path = self.normal_spaces(self.cleanse(stripped_path))
        tokens = self.filter_tokens([_t.lower()
                                     for _t in normal_path.split(" ")])
        return ParsedAudioFilePath(path=path,
                                   key=key_sig,
                                   tempo=tempo,
                                   tokens=tokens)

    def normal_spaces(self, path: str) -> str:
        for _d in SPACE_ALTS:
            path = path.replace(_d, " ")
        path = path.strip()
        while "  " in path:
            path = path.replace("  ", " ")
        return path

    def trim(self, path: str) -> str:
        if "." in path:
            path = ".".join(path.split(".")[:-1])
        if "/" in path:
            path = "/".join(path.split("/")[1:])
        return path

    def cleanse(self, path: str) -> str:
        for _c in DROP_CHARS:
            path = path.replace(_c, "")
        for acronym in self.acronyms:
            path = path.replace(acronym, "")
        return path

    def parse_key_signature(self, space_normal_path: str) -> Tuple[str]:
        raw_key = self.raw_key_signature(space_normal_path)
        key_sig = self.normal_key_signature(raw_key) if raw_key else ""
        return (raw_key, key_sig)

    def raw_key_signature(self, path: str) -> str:
        result = re.search(REGEX_RAW_KEY, path)
        return result.group() if result else ""

    def normal_key_signature(self, raw_keysig: str) -> str:
        if len(raw_keysig) < 2:
            return raw_keysig.upper()
        signature = raw_keysig[0].upper() + raw_keysig[1:].lower()
        signature = signature.replace(" ", "")
        for substr in KEY_SUBSTITUTIONS.keys():
            signature = re.sub(substr, KEY_SUBSTITUTIONS[substr], signature)
        return signature

    def parse_tempo(self, space_normal_path: str) -> Tuple[str]:
        candidate_raw_tempos = self.raw_tempo_candidates(space_normal_path)
        if not candidate_raw_tempos:
            raw_tempo, tempo = "", ""
        elif len(candidate_raw_tempos) == 1:
            raw_tempo = candidate_raw_tempos[0]
            tempo = self.normal_tempo(raw_tempo)
        else:
            raw_tempo, tempo = self.tempo_from_candidates(candidate_raw_tempos)
        return (raw_tempo, tempo)
    
    def has_tempo_label(self, path: str) -> bool:
        return True if "bpm" in path.lower() else False

    def raw_tempo_candidates(self, path: str) -> List[str]:
        if self.has_tempo_label(path):
            return self.labeled_raw_tempo(path)
        else:
            return self.unlabeled_raw_tempo(path)

    def normal_tempo(self, raw_tempo: str) -> str:
        result = re.search(r"\d{2,3}", raw_tempo)
        return result.group() if result else ""

    def labeled_raw_tempo(self, path: str) -> List[str]:
        candidates = []
        result = re.search(REGEX_RAW_TEMPO_POSTFIX, path)
        result_inv = re.search(REGEX_RAW_TEMPO_PREFIX, path)
        if result:
            candidates.append(result.group())
        if result_inv:
            candidates.append(result_inv.group())
        return candidates
        
    def unlabeled_raw_tempo(self, path: str) -> List[str]:
        candidates = re.findall(REGEX_RAW_TEMPO_NO_LABEL, path)
        if len(candidates) > 1:
            candidates = list(set(candidates))
        return candidates

    def tempo_from_candidates(self, candidates: List[str]) -> Tuple[str]:
        numbers = [int(self.normal_tempo(_c)) for _c in candidates]
        for tempo_range in [RANGE_TEMPO_1, RANGE_TEMPO_2, RANGE_TEMPO_3]:
            indexed_tempos = [(_i, _n) for _i, _n in enumerate(numbers)
                              if tempo_range[0] <= _n <= tempo_range[1]]
            if len(indexed_tempos) == 1:
                break
        if len(indexed_tempos) != 1:
            return ("", "")
        raw_tempo = candidates[indexed_tempos[0][0]]
        normal_tempo = indexed_tempos[0][1]
        if (normal_tempo < RANGE_TEMPO_4[0] or normal_tempo > RANGE_TEMPO_4[1]):
            return ("", "")
        return (raw_tempo, normal_tempo)

    def asset_acronyms(self, cname: str) -> List[str]:
        label, title, _ = NameUtility.divide_cname(cname)

        if title.split(" ")[-1].isnumeric():
            title_without_vol = " ".join(title.split(" ")[:-1])
            acronyms_without_vol = self.asset_acronyms(f"{label} - {title_without_vol}")
            acronyms_with_vol= [f"{_a}{title.split(' ')[-1]}"
                                for _a in acronyms_without_vol]
            acronyms_with_vol_label = [f"{_a}V{title.split(' ')[-1]}"
                                       for _a in acronyms_without_vol]
            return acronyms_with_vol + acronyms_without_vol + acronyms_with_vol_label

        label_acronym = "".join([_i[0] for _i in label.split(" ")])
        title_acronym = "".join([_i[0] for _i in title.split(" ")])
        acronyms = []
        for acronym in [label_acronym, title_acronym]:
            acronyms.append(acronym) if len(acronym) > 1 else None
        if label_acronym and title_acronym:
            acronyms.append(f"{label_acronym}{title_acronym}")
        return acronyms
        
    def filter_tokens(self, tokens: List[str]) -> List[str]:
        return self.drop_spam_tokens(
               self.drop_nonlinguistic_tokens(
               self.drop_single_char_tokens(
               tokens
               )))

    def drop_single_char_tokens(self, tokens: List[str]) -> List[str]:
        return [_t for _t in tokens if len(_t) > 1]

    def drop_spam_tokens(self, tokens: List[str]) -> List[str]:
        return [_t for _t in tokens if _t != _t[0]*len(_t)]
    
    def drop_nonlinguistic_tokens(self, tokens: List[str]) -> List[str]:
        return [_t for _t in tokens if re.search(r"[a-z]", _t)]
    