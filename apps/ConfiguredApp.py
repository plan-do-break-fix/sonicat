
from contextlib import closing
from dataclasses import dataclass
from yaml import load, SafeLoader


@dataclass
class Config:
    app_name: str
    log_level: str
    data: str
    managed: str
    logging: str
    intake: str
    export: str
    temp: str
    #root: str
    #config: str
    #covers: str
    #recover: str


class App:

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        with closing(open(f"{sonicat_path}/catalog/catalog-config.yml", "r")) as _f:
            #self.cfg = Config(**load(_f.read(), SafeLoader))
            params = load(_f.read(), SafeLoader)[app_key]
        config = {
            "app_name": params["moniker"],
            "log_level": params["log_level"],
            "data": f"{sonicat_path}/catalog/data",
            "managed": params["path"]["managed"],
            "log": f"{sonicat_path}/catalog/log",
            "intake": params["path"]["intake"],
            "export": params["path"]["export"],
            "temp": "/tmp"
        }
        self.cfg = Config(**config)
