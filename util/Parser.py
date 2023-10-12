
from dataclasses import dataclass
import re
from typing import Dict, List, Tuple

from util.NameUtility import NameUtility

REGEX_VERSION = r"(?<=v)(\d+\.)+(?=\b)"
REGEX_RAW_KEY = r"(?<=\b)[a-g] ?(b|#|sharp|flat)? ?(m(in|aj)?)?(or)?([2-9])?(b|#)?((sus|dim)[2-9]?)?(?=(\s|\.|$))"
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
    "+",
    ",",
    ".",
    ":",
    "|", "︱",
    "(",")",
    "[","]",
    "{","}",
    "<",">"
]
DROP_CHARS = [
    "'", "\"", "!", "?", ";", "^", "°", "*", "`", "’", "#"
]
ALPHA_NUM_TRANSITION_REGEX = r"(\d+)" 

# string subs to be run AFTER space alt subs
STRING_SUBSTITUTIONS = {
    #"breakbeat": ["break beat"],
    #"drumkit": ["drum kit"],
    #"loopkit": ["loop kit"],
    "groove": ["grv"],
    "guitar": ["gtr"],
    "hihat": ["hi hat", "hh"],
    "hiphop": ["hip hop"],
    "lofi": ["lo fi"],
    "oneshot": ["one shot"],
    "": ["&amp", "&quot"]
}
KEY_SUBSTITUTIONS = {
    "sharp": "#",
    "flat": "b",
    "or": "",
    "maj": "",
    r"m($|[2-9])": "min"
}
# These are most commonly used in English words for decorative purposes
CHAR_SUBSTITUTIONS = {
    "a": ["ä"],
    "e": ["ë", "é", "ê"],
    "i": ["ï"],
    "n": ["ñ"],
    "o": ["ö"],
    "s": ["$", "§"],
    "z": ["ž"] 
}
COMPOUND_TOKEN_ENDINGS = [
    "bass", "basses",
    "beat", "beats",
    "brass",
    "break", "breaks",
    "chord", "chords",
    "clap", "claps",
    "crash", "crashes",
    "cymbal", "cymbals",
    "drum", "drums",
    "effect",
    "fast",
    "fill", "fills",
    "funk",
    "fx",
    "groove", "grooves",
    "guitar", "guitars",
    "hihat", "hihats",
    "hat", "hats",
    "high",
    "hit", "hits",
    "horn", "horns",
    "jam", "jams",
    "keyboard", "keyboards",
    "keys",
    "kick", "kicks",
    "kit",
    "loop", "loops",
    "major",
    "melody",
    "minor",
    "oneshot", "oneshots",
    "organ", "organs",
    "pad", "pads",
    "piano", "pianos",
    "perc", "percs",
    "percussion",
    "pluck", "plucks",
    "rhythm",
    "riff", "riffs",
    "roll", "rolls",
    "shot", "shots",
    "slow",
    "snare", "snares",
    "sound", "sounds",
    "stab", "stabs",
    "stomp", "stomps",
    "strings",
    "synth", "synths",
    "tom", "toms",
    "vox"
]
ALLOWED_NUMERIC_TOKENS = ["50", "60", "70", "80", "90",
                          "303", "404", "505", "606", "707", "808", "909"
                          ]
MIN_TOKEN_LEN = 3

@dataclass
class ParsedAudioFilePath:
    path: str
    key: str
    tempo: str
    tokens: List[str]


class AudioFilePathParser:

    def __init__(self) -> None:
        self.audio_exts = ["aif", "aiff", "flac", "mid", "midi", "mp3", "ogg", "wav"]
        #self.cache = {}
        #self.acronyms = []

    def parse_path(self, path: str) -> ParsedAudioFilePath:
        path = self.trim(path.lower())
        space_normal_path = self.normal_spaces(path)
        raw_tempo, tempo = self.parse_tempo(space_normal_path)
        raw_key, key_sig = self.parse_key_signature(space_normal_path)
        stripped_path = path.replace(raw_tempo, "").replace(raw_key, "")
        normal_path = self.normal_spaces(self.cleanse(stripped_path))
        tokens = [_t.lower() for _t in normal_path.split(" ")]
        tokens = self.filter_tokens(
                 #self.split_tokens_by_ending(
                 self.split_alphanumeric_tokens(
                    tokens
                 )
                 #)
                 )
        return ParsedAudioFilePath(path=path,
                                   key=key_sig,
                                   tempo=tempo,
                                   tokens=tokens)

    def normal_spaces(self, path: str) -> str:
        for _d in SPACE_ALTS:
            path = path.replace(_d, " ")
        while "  " in path:
            path = path.replace("  ", " ")
        path = path.strip()
        return path

    def trim(self, path: str) -> str:
        """
        #Returns path without root directory or file extension.
        File extensions do not need to be parsed.
        #The root directory is the asset name, and need only be parsed once.
        """
        if "." in path:
            path = ".".join(path.split(".")[:-1])
        if "/" in path:
            path = "/".join(path.split("/")[1:])
        return path

    def cleanse(self, path: str) -> str:
        for _c in DROP_CHARS:
            path = path.replace(_c, "")
        #for acronym in self.acronyms:
        #    path = path.replace(acronym, "")
        path = self.make_substitutions(CHAR_SUBSTITUTIONS, path)
        path = self.make_substitutions(STRING_SUBSTITUTIONS, path)
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
        if (int(normal_tempo) < RANGE_TEMPO_4[0] or int(normal_tempo) > RANGE_TEMPO_4[1]):
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
               self.drop_tokens_by_len(
               #self.untag_hashtags(
               self.drop_attribution_tokens(
                   tokens
               )
               #)
               )))

    def drop_tokens_by_len(self, tokens: List[str]) -> List[str]:
        return [_t for _t in tokens if len(_t) >= MIN_TOKEN_LEN]

    def drop_spam_tokens(self, tokens: List[str]) -> List[str]:
        return [_t for _t in tokens if _t != _t[0]*len(_t)]
    
    def split_alphanumeric_tokens(self, tokens: List[str]) -> List[str]:
        output = []
        for _t in tokens:
            output += [_i for _i
                       in re.split(ALPHA_NUM_TRANSITION_REGEX, _t)
                       if _i]
        return output

    #def untag_hashtags(self, tokens: List[str]) -> List[str]:
    #    output = []
    #    for _t in tokens:
    #        if (_t.startswith("#") and "#" not in _t[1:]):
    #            output.append(_t[1:])
    #        else:
    #            output.append(_t)
    #    return output
        
    def drop_nonlinguistic_tokens(self, tokens: List[str]) -> List[str]:
        return [_t for _t in tokens
                if (re.search(r"[a-z]", _t)
                or _t in ALLOWED_NUMERIC_TOKENS)
                ]
    
    def drop_attribution_tokens(self, tokens: List[str]) -> List[str]:
        return [_t for _t in tokens if not _t.startswith("@")]
    
    def make_substitutions(self, subs_dict: Dict[str, List[str]],
                                 text: str
                                 ) -> str:
        for _k in subs_dict.keys():
            for _i in subs_dict[_k]:
                if _i in text:
                    text = text.replace(_i, _k)
        return text
    
    def split_tokens_by_ending(self, tokens: List[str]) -> List[str]:
        output = []
        for _t in tokens:
            current_out = len(output)
            for _ending in COMPOUND_TOKEN_ENDINGS:
                if _t.endswith(_ending) and _t != _ending:
                    split_at = (0 - len(_ending))
                    output += [_t[:split_at], _ending]
                    break
            if len(output) == current_out:
                output.append(_t)
        if len(output) > len(tokens):
            return self.split_tokens_by_ending(output)
        return output
            
    def reduce_plural_tokens(self, tokens: List[str]) -> List[str]:
        output = []
        for _t in tokens:
            if _t.endswith("ies") and _t[:-3] in COMPOUND_TOKEN_ENDINGS:
                output.append(_t[:-3])
            elif _t.endswith("es") and _t[:-2] in COMPOUND_TOKEN_ENDINGS:
                output.append(_t[:-2])
            elif _t.endswith("s") and _t[:-1] in COMPOUND_TOKEN_ENDINGS:
                output.append(_t[:-1])
            else:
                output.append(_t)
        return output