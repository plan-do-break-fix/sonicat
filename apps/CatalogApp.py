#!/usr/bin/python3
from yaml import load
try:
  from yaml import CSafeLoader as SafeLoader
except ImportError:
  from yaml import SafeLoader

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
import shutil
from typing import Dict

from interfaces.Catalog import DatabaseInterface as CatalogDB
from util.FileUtility import Archive, Inventory, Handler
from util import Logs
from util.NameUtility import Transform, Validate

"""

Accession.intake() is used to add new assets to the catalog
  - Runs on the intake folder
  - Accesses each canonically-named folder as an asset
  - Calls Archive.archive()
  - Calls FileHandler.sort_assets()
  - Intended to be called regularly/programmatically

Accession.rebuild_catalog() is used to rebuild the catalog from current assets
  - Runs on the catalog ROOT
  - Flushes the current catalog, if it exists
  - Iterates over each label directory in ROOT
  - For each canonically-named rar file
    - Calls Archive.restore()
    - Assess the restored folder as as asset
    - Calls Archive.archive()
  - Intended to be called once, and even that may be too much.

"""

@dataclass
class Config:
    root: str
    data: str
    logging: str
    covers: str
    intake: str


class AccessionApp:

    def __init__(self, config_path: str) -> None:
        with closing(open(config_path, "r")) as _f:
            self.cfg = Config(**load(_f.read(), SafeLoader))


class Build(AccessionApp):
    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)
        date_str = datetime.now().strftime('%Y-%m-%d')
        log_path = f"{self.cfg.logging}/{date_str}-CatalogApp.log"
        self.inventory = Inventory(f"{self.cfg.data}/catalog.sqlite", log_path)
        self.log = Logs.initialize_logging("CatalogApp", log_path)
        self.log.info("Build Catalog application initialized.")
    
    def run(self) -> bool:
        labels = [_d for _d in shutil.os.listdir(self.cfg.root)
                  if not _d.startswith(".")]
        self.log.info(f"Processing {len(labels)} label directories.")
        for label_dir in labels:
            self.log.info(f"Begin processing directory {label_dir}.")
            label_path = f"{self.cfg.root}/{label_dir}"
            asset_archives = [_r for _r in 
                              shutil.os.listdir(label_path)
                              if _r.endswith(".rar")
                              and Validate.name_is_canonical(_r.split(".")[0])
                              ]
            self.log.info(f"Processing {len(asset_archives)} asset archives.")
            for archive in asset_archives:
                cname = archive.split(".")[0]
                self.log.info(f"Begin processing asset {cname}.")
                Archive.restore(f"{label_path}/{archive}")
                if not shutil.os.path.isdir(f"{label_path}/{cname}"):
                    self.log.error(f"Restoration of archive {archive} failed.")
                    raise RuntimeError(f"Failed to restore {archive}.")
                shutil.move(f"{label_path}/{archive}", f"{label_path}/{archive}.old")
                if self.inventory.add_asset(f"{label_path}/{cname}"):
                    asset_id = self.inventory.db.asset_id(cname)
                    self.log.info(f"{cname} successfully inventoried as ID {asset_id}.")
                if Archive.archive(f"{label_path}/{cname}"):
                    self.log.info(f"{cname} archived as {archive}.")
                shutil.rmtree(f"{label_path}/{cname}")
                _digest = self.inventory.digest(f"{label_path}/{archive}")
                if self.inventory.db.update_asset_digest(asset_id, _digest):
                    self.log.info(f"Hash digest updated for asset ID {asset_id}.")
                self.log.info(f"Processing complete for {cname}.")
            self.log.info(f"Processing complete for label directory {label_dir}.")
        return True


class Update(AccessionApp):

    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)
        self.handler = Handler()

    def run(self) -> bool:
        labels = [_d for _d in shutil.os.listdir(self.cfg.intake)
                  if not _d.startswith(".")]
        for label_dir in labels:
            assets = [_d for _d in 
                      shutil.os.list(f"{self.cfg.root}/{label_dir}")
                      if shutil.os.path.isdir(_d)
                      and Validate.name_is_canonical(_d)
                      ]
            for cname in assets:
                self.inventory.add_asset(f"{self.cfg.data}/{cname}")
                Archive.archive(f"{self.cfg.data}/{cname}")

        self.handler.sort_assets(self.cfg.intake, self.cfg.root)

        return True
