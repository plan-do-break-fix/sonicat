
from contextlib import closing
from yaml import load, SafeLoader

from interfaces.database.Catalog import ReadInterface
from util import Logs

from typing import Any, Dict, List


"""
CONVENTION: name, moniker
'Name' refers to a snake_case string
  example usage: dictionary keys, directory names
'Moniker' refers to a CamelCase string
  example usage: logging, file names
"""


class AppConfig:

    def __init__(self, sonicat_path: str, app_name: str, debug_cfg=None) -> None:
        self.sonicat_path = sonicat_path
        if app_name == "debug":
            raw_cfg = debug_cfg
        else:
            with closing(open(f"{self.sonicat_path}/config/config.yaml", "r")) as _f:
                raw_cfg = load(_f.read(), SafeLoader)
        self.catalog_cfg = raw_cfg["catalogs"]
        self.app_cfg = raw_cfg["apps"]
        if app_name == "tasks":
            self.task_cfg = raw_cfg["tasks"]
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
    
    def __init__(self, config: AppConfig) -> None:
        self.cfg = config
        self.log = Logs.initialize_logging(
                                           self.cfg.moniker,
                                           self.cfg.log_level,
                                           self.cfg.logpath()
                                           )
        self.replicas = {}
    
    def load_catalog_replicas(self, catalog_names=[]) -> bool:
        """
        Creates connections to all read-replica catalog database instances.
        """
        if not catalog_names:
            catalog_names = self.cfg.catalog_names()
        elif not all([_c in self.cfg.catalog_names() for _c in catalog_names]):
            raise ValueError
        for catalog_name in catalog_names:
            moniker = self.cfg.catalog_cfg[catalog_name]["moniker"]
            dbpath = f"{self.cfg.sonicat_path}/data/catalog/{moniker}.sqlite"
            self.replicas[catalog_name] = ReadInterface(dbpath)
        return 

  # Methods to be reimplemented by subclasses
    def run_task(self, task: Dict) -> Dict:
        raise RuntimeError
        result = None
        task["result"] = self.encode_result(result)
        return task

    def load_data_replicas(self) -> bool:
        raise RuntimeError
        self.replicas["replica"] = None


    #  This needs to be implemented in each subclass
    #def load_appdata_replicas(self, app_names=[]) -> bool:
    #    if not app_names:
    #        app_names = self.cfg.app_names()
    #    elif not all([_a in self.cfg.app_names() for _a in app_names]):
    #        raise ValueError
    #    for app_name in app_names:
    #        app_type = self.cfg.type_of_app(app_name)
    #        moniker = self.cfg.app_cfg[app_name]
    #        dbpath = f"{self.cfg.sonicat_path}/data/{app_type}/{moniker}.sqlite"
    #        self.replicas[app_name] = ????
        
    #def encode_result(self, result: Any) -> Dict:
    #    raise RuntimeError
    #    return {}




