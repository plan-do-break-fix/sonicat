
from contextlib import closing
from yaml import load, SafeLoader

from interfaces.database.Catalog import ReadInterface, WriteInterface
from util import Logs

from typing import Dict, List


"""
CONVENTION: name, moniker
'Name' refers to a snake_case string
  example usage: dictionary keys, directory names
'Moniker' refers to a CamelCase string
  example usage: logging, file names
"""


class AppConfig:

    def __init__(self, sonicat_path: str, app_name: str, manual_cfg=None) -> None:
        if app_name == "test":
            raw_cfg = manual_cfg
        else:
            with closing(open(f"{self.cfg.sonicat_path}/config/config.yaml", "r")) as _f:
                raw_cfg = load(_f.read(), SafeLoader)
        self.catalog_cfg = raw_cfg["catalogs"]
        self.app_cfg = raw_cfg["apps"]
        self.sonicat_path = sonicat_path
        self.app_name = app_name
        self.app_type = self.type_of_app(app_name)
        if not self.app_type:
            raise ValueError
        self.app_moniker = raw_cfg["apps"][self.app_type][app_name]["moniker"]
        self.log_level = raw_cfg["apps"][self.app_type][app_name]["log_level"]

    def type_of_app(self, app_name: str) -> str:
        for _t in list(self.app_cfg.keys()):
            if app_name in list(self.app_cfg[_t].keys()):
                return _t
        return ""

    def catalog_names(self) -> List[str]:
        return list(self.catalog_cfg.keys())

    def app_names(self, app_types=[]) -> List[str]:
        if not app_types:
            app_types = list(self.app_cfg.keys())
        elif not all([_t in self.app_cfg.keys() for _t in app_types]):
            return []
        app_names = []
        for _t in app_types:
            app_names += list(self.app_cfg[_t].keys())
        return app_names

    def data_path(self) -> str:
        return f"{self.sonicat_path}/data/{self.app_type}"

    def log_path(self) -> str:
        return f"{self.sonicat_path}/log/{self.app_type}"

    def temp_path(self) -> str:
        return f"/tmp/sonicat-{self.app_moniker}"


class SimpleApp:

    """Base class for all configured Apps"""

    def __init__(self, sonicat_path: str, app_type: str, app_name: str) -> None:
        with closing(open(f"{sonicat_path}/config/config.yaml", "r")) as _f:
            self.cfg = load(_f.read(), SafeLoader)
        self.app_name = app_name
        self.cfg["sonicat_path"] = sonicat_path
        self.temp = f"/tmp/sonicat-{app_name}"
        logpath = f"{sonicat_path}/log/{app_type}/"
        self.log = Logs.initialize_logging(app_name, "debug", logpath)
        self.writable, self.replicas = {}, {}
    
    def load_catalog_replicas(self, catalog_names=[]) -> bool:
        """
        Creates connections to all read-replica catalog database instances.
        """
        if not catalog_names:
            catalog_names = self.cfg.catalog_names()
        elif not all([_c in self.cfg.catalog_names() for _c in catalog_names]):
            raise ValueError
        for catalog_name in catalog_names:
            moniker = self.cfg.catalogs[catalog_name]['moniker']
            dbpath = f"{self.cfg.sonicat_path}/data/catalog/{moniker}.sqlite"
            self.replicas[catalog_name] = ReadInterface(dbpath)
        return 
    
    def load_appdata_replicas(self, app_names=[]) -> bool:
        pass



    def load_catalog_writable(self, catalog_name: str) -> bool:
        """
        Create read/write connection to indicated catalog database.
        """
        if catalog_name in self.writable.keys():
            return False
        moniker = self.cfg['catalogs'][catalog_name]['moniker']
        db_path = f"{self.cfg['sonicat_path']}/data/catalog/{moniker}.sqlite"
        self.writable[catalog_name] = WriteInterface(db_path)
        self.log.debug(f"Established writable connected to {db_path}.")
        return True

    

class AppRunner:

    pass