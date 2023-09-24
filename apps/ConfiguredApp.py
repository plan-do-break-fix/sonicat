
from contextlib import closing
from dataclasses import dataclass
from yaml import load, SafeLoader


@dataclass
class Config:
    name: str
    app_key: str
    log_level: str
    data: str
    managed: str
    log: str
    intake: str
    export: str
    temp: str
    #root: str
    #config: str
    #covers: str
    #recover: str


class App:

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        with closing(open(f"{sonicat_path}/config/config.yaml", "r")) as _f:
            #self.cfg = Config(**load(_f.read(), SafeLoader))
            params = load(_f.read(), SafeLoader)[app_key]
        config = {
            "name": params["moniker"],
            "app_key": app_key,
            "log_level": params["log_level"],
            "data": f"{sonicat_path}/data",
            "managed": params["path"]["managed"],
            "log": f"{sonicat_path}/log",
            "intake": params["path"]["intake"],
            "export": params["path"]["export"],
            "temp": f"/tmp/sonicat-{app_key}"
        }
        self.cfg = Config(**config)
