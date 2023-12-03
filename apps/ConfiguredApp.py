
from contextlib import closing
from yaml import load, SafeLoader

from interfaces.database.Catalog import ReadInterface, WriteInterface
from util import Logs


class SimpleApp:

    """Base class for all Sonicat Apps"""

    def __init__(self, sonicat_path: str, app_type: str, app_name: str) -> None:
        with closing(open(f"{sonicat_path}/config/config.yaml", "r")) as _f:
            self.cfg = load(_f.read(), SafeLoader)
        self.app_name = app_name
        self.cfg["sonicat_path"] = sonicat_path
        self.temp = f"/tmp/sonicat-{app_name}"
        logpath = f"{sonicat_path}/log/{app_type}/"
        self.log = Logs.initialize_logging(app_name, "debug", logpath)
        self.writable, self.replicas = {}, {}
    
    def load_catalog_replicas(self) -> bool:
        """
        Creates connections to all read-replica catalog database instances.
        """
        if self.replicas:
            return False
        for catalog_name in list(self.cfg["catalogs"].keys()):
            moniker = self.cfg['catalogs'][catalog_name]['moniker']
            dbpath = f"{self.cfg['sonicat_path']}/data/catalog/{moniker}.sqlite"
            self.replicas[catalog_name] = ReadInterface(dbpath)
        self.log.debug(f"Established connection to catalog replicas.")
        return True

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

    