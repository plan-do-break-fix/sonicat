
from contextlib import closing
from dataclasses import dataclass
from yaml import load, SafeLoader


@dataclass
class Config:
    name: str
    moniker: str
    log_level: str
    data: str
    managed: str
    log: str
    intake: str
    export: str
    temp: str



class App:

    def __init__(self, sonicat_path: str, moniker: str) -> None:
        with closing(open(f"{sonicat_path}/config/config.yaml", "r")) as _f:
            self.config = load(_f.read(), SafeLoader)
        self.catalog_names = list(self.config["catalogs"].keys())
        if not moniker:
            self.cfg = Config(
                log_level="debug",
                data=f"{sonicat_path}/data",
                log=f"{sonicat_path}/log",
                name="",
                moniker="",
                managed="",
                intake="",
                export="",
                temp="/tmp/sonicat"
            )
        else:
            app_params = self.config["catalogs"][moniker]
            app_config = {
                "name": app_params["moniker"],
                "moniker": moniker,
                "log_level": app_params["log_level"],
                "data": f"{sonicat_path}/data",
                "managed": app_params["path"]["managed"],
                "log": f"{sonicat_path}/log",
                "intake": app_params["path"]["intake"],
                "export": app_params["path"]["export"],
                "temp": f"/tmp/sonicat-{moniker}"
            }
            self.cfg = Config(**app_config)
