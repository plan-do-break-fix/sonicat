
import re
from typing import List, Tuple

#ENG_STOPWORDS = ["and", "in", "of", "the"]
#DOMAIN_STOPWORDS = [
#                    "audio",
#                    "loops",
#                    "music",
#                    "records",
#                    "samples",
#                    "sound",
#                    "sounds",
#                    "studio",
#                    "studios"
#                    ]


class NameUtility:

    @staticmethod
    def name_is_canonical(name: str) -> bool:
        """
        Returns True if asset FS name conforms to <Publisher> - <Title> naming convention.
        """
        name = name.replace(".rar", "")
        return all([
            len(name.split(" - ")) > 1,
            not name.startswith(" "),
            not name.endswith(" "),
            not any(["." in name,
                    "  " in name
                    ])
            ])

    @staticmethod
    def divide_cname(cname: str) -> Tuple[str]:
        """
        Return tuple of Label, Title and Note components of a canonical name.
        Input can be asset root directory name or name of asset archive file.
        """
        if cname.endswith(".rar"):
            cname = cname.replace(".rar", "")
        parts = cname.split(" - ")
        label, title = parts[0], " - ".join(parts[1:])
        if " (" in title and title.endswith(")"):
            note = title.split(" (")[-1][:-1]
            title = title.replace(f" ({note})", "")
        else:
            note = ""
        return (label, title, note)
    
    @staticmethod
    def file_extension(fname: str) -> str:
        ext = fname.split(".")[-1]
        if any([ext == "",
                ext == fname,
                " " in ext,
                any([f"{_p}{ext}" == fname for _p in [".", "_.", "._."]])
                    ]):
            return ""
        return ext

    @staticmethod
    def cname_tokens(cname: str) -> List[str]:
        tokens, parts = [], NameUtility.divide_cname(cname)
        for _part in parts:
            tokens += _part.split(" ")
        return tokens

    @staticmethod
    def label_dir_from_cname(cname: str) -> str:
        """
        Return the name of the label directory associated with a canonically-named asset.
        """
        return cname.split(" - ")[0].lower().replace(" ", "_")

    @staticmethod
    def title_has_media_type_label(title: str) -> bool:
        return any([_l in title for _l in [" CDM", "CDR", "CDS", " MCD", " EP", " LP"]])

    @staticmethod
    def drop_media_type_labels(self, title: str) -> str:
        PATTERNS = [
            r"\b(MCD|CD(M|M?S|R))\d?\b",
            r"\b[EL]P\d?\b"
        ]
        for _p in PATTERNS:
            title = re.sub(_p, "", title)
        return title


class Variations(NameUtility):

    def name_variations(self, name: str) -> List[str]:
        if " - " in name:
            names = [name.replace(" - ", " ")]
            for part in name.split(" - "):
                names.append(part)
        else:
            names = [name]
        alt_spaces = [_a for _alts in
                       [self.insert_optional_spaces(_n)
                       for _n in names]
                       for _a in _alts]
        stripped = [_a for _alts in
                    [self.strip_stopwords(_n) 
                    for _n in names
                    if self.strip_stopwords(_n) != _n]
                    for _a in _alts]
        return names + alt_spaces + stripped

    def stub_forms(self, variations: List[str]) -> List[str]:
        """
        Returns common stub forms of <name>, in whole and parts if applicable.
        """
        forms = []
        for _v in variations:
            forms += self.join_tokens(self.tokenize(_v))
        return forms
    
    #def insert_optional_spaces(self, name: str) -> str:
    #    allowed = [("drumkit", "drum kit"),
    #               ("hiphop", "hip hop"),
    #               ("lofi", "lo fi")
    #               ]
    #    for _t in allowed:
    #        if _t[0] in name:
    #            name = name.replace(_t[0], _t[1])
#
    #def tokenize(self, name: str) -> List[str]:
    #    """
    #    Returns list of tokens from a publisher or product name.
    #    """
    #    if " - " in name:
    #        name = name.replace(" - ", " ")
    #    return name.lower().split(" ")
    #   
    #def join_tokens(self, tokens: List[str], connectors=["", "-"]) -> List[str]:
    #    """
    #    Returns the string formed by joining <tokens> with a given connector, for each connector in connectors.
    #    """
    #    if len(tokens) == 1:
    #        return tokens
    #    output = []
    #    for _c in connectors:
    #        output.append(_c.join(tokens))
    #    return output
    #
    #def drop_stopwords(self, tokens: List[str]):
    #    """
    #    Returns <tokens> without any members also found in a stopwords list.
    #    """
    #    to_drop = ENG_STOPWORDS + DOMAIN_STOPWORDS
    #    return [_t for _t in tokens if _t not in to_drop]
#
    #def strip_stopwords(self, text: str):
    #    """
    #    Returns <text> with all stopwords removed.
    #    """
    #    tokens = text.split(" ")
    #    tokens = self.drop_stopwords(tokens)
    #    return " ".join(tokens)