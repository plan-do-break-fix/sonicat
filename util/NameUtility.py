

from typing import List, Tuple

ENG_STOPWORDS = ["and", "in", "of", "the"]
DOMAIN_STOPWORDS = [
                    "audio",
                    "loops",
                    "music",
                    "records",
                    "samples",
                    "sound",
                    "sounds",
                    "studio",
                    "studios"
                    ]


class Validate:

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


class Transform:

    @staticmethod
    def divide_cname(cname: str) -> Tuple[str]:
        cname = cname.replace(".rar", "")
        parts = cname.split(" - ")
        label, title = parts[0], " - ".join(parts[1:])
        if " (" in title:
            note = title.split(" (")[-1][:-1]
        else:
            note = ""
        return (label, title, note)

    def name_variations(self, name: str) -> List[str]:
        if " - " in name:
            names = [name.replace(" - ", " ")]
            for part in name.split(" - "):
                names.append(part)
        alt_spaces = [_a for _alts in
                       [self.add_allowed_spaces(_n)
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
    
    def add_allowed_spaces(self, name: str) -> str:
        allowed = [("drumkit", "drum kit"),
                   ("hiphop", "hip hop"),
                   ("lofi", "lo fi")
                   ]
        for _t in allowed:
            if _t[0] in name:
                name = name.replace(_t[0], _t[1])

    def tokenize(self, name: str) -> List[str]:
        """
        Returns list of tokens from a publisher or product name.
        """
        if " - " in name:
            name = name.replace(" - ", " ")
        return name.lower().split(" ")
       
    def join_tokens(self, tokens: List[str], connectors=["", "-"]) -> List[str]:
        """
        Returns the string formed by joining <tokens> with a given connector, for each connector in connectors.
        """
        if len(tokens) == 1:
            return tokens
        output = []
        for _c in connectors:
            output.append(_c.join(tokens))
        return output
    
    def drop_stopwords(self, tokens: List[str]):
        """
        Returns <tokens> without any members also found in a stopwords list.
        """
        to_drop = ENG_STOPWORDS + DOMAIN_STOPWORDS
        return [_t for _t in tokens if _t not in to_drop]

    def strip_stopwords(self, text: str):
        """
        Returns <text> with all stopwords removed.
        """
        tokens = text.split(" ")
        tokens = self.drop_stopwords(tokens)
        return " ".join(tokens)