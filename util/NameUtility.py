

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

    def name_forms(self, name: str) -> List[str]:
        """
        Returns common stub forms of <name>, in whole and parts if applicable.
        """
        if " - " in name:
            name_parts, partial_forms = name.split(" - "), []
            for _part in name_parts:
                for _form in self.name_forms(_part):
                    partial_forms.append(_form)
            name = name.replace(" - ", " ")
        else:
            partial_forms = []
        cleansed = name.replace("-", " ")  #why a space instead of nothing? if "a-b" becomes "a b", then all combined forms will be generated. If it becomes "ab", that it all it will ever be.
        forms = self.join_tokens(self.tokenize(cleansed))
        return forms + partial_forms if partial_forms else forms
               
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