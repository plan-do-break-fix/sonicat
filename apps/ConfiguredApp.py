
from contextlib import closing
from yaml import load, SafeLoader

from apps.Helpers import Config


class App:

    def __init__(self, config_path: str) -> None:
        with closing(open(config_path, "r")) as _f:
            self.cfg = Config(**load(_f.read(), SafeLoader))
        
