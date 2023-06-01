
from contextlib import closing
from dataclasses import dataclass
import json
import shutil
from typing import Dict



@dataclass
class Config:
    app_name: str
    root: str
    data: str
    logging: str
    log_level: str
    covers: str
    intake: str


class CheckPoint:

    def __init__(self, config: Config) -> None:
        checkpoint_dir = f"{config.data}/checkpoints"
        self.path = f"{config.data}/checkpoints/{config.app_name}"
        self.major, self.minor = "", ""
        if not shutil.os.path.exists(checkpoint_dir):
            shutil.os.path.mkdir(checkpoint_dir)
            return None

    def read(self) -> Dict:
        with closing(open(self.path ,"r")) as _f:
            data = json.load(_f.read())
        return data

    def write(self) -> bool:
        data = {"major": self.major, "minor": self.minor}
        with closing(open(self.path ,"w")) as _f:
            json.dump(data, _f)
        return True

    def set(self, major="", minor="") -> bool:
        if major:
            self.major, self.minor = major, ""
        elif minor:
            self.minor = minor
        else:
            raise ValueError("Checkpoint.set called with no arguments.")
        self.write()

    def purge(self) -> bool:
        shutil.os.remove(self.path)
        self.major, self.minor = "", ""