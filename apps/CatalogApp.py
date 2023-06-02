#!/usr/bin/python3

from contextlib import closing
from datetime import datetime
import shutil
from typing import List
from yaml import load, SafeLoader

from apps.Helpers import CheckPoint, Config
from util.FileUtility import Archive, Inventory, Handler
from util import Logs
from util.NameUtility import Validate

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


class AccessionApp:

    def __init__(self, config_path: str) -> None:
        with closing(open(config_path, "r")) as _f:
            self.cfg = Config(**load(_f.read(), SafeLoader))
        self.chkpt = CheckPoint(self.cfg)


class Build(AccessionApp):
    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)
        self.log = Logs.initialize_logging(self.cfg)
        self.inventory = Inventory(self.cfg, self.log)
        self.log.info("Build Catalog application initialized.")
    
    def run(self) -> bool:
        labels = [_d for _d in shutil.os.listdir(self.cfg.root)
                  if not _d.startswith(".")]
        if self.chkpt.major:
            self.log.info(f"Major checkpoint loaded - Skipping to {self.chkpt.major}...")
            labels = self.skip_to(labels, self.chkpt.major)
        self.log.info(f"Processing {len(labels)} label directories")
        for label_dir in labels:
            self.log.info(f"Begin processing directory: {label_dir}")
            label_path = f"{self.cfg.root}/{label_dir}"
            asset_archives = [_r for _r in 
                              shutil.os.listdir(label_path)
                              if _r.endswith(".rar")
                              and Validate.name_is_canonical(_r.split(".")[0])
                              ]
            if self.chkpt.minor:
                self.log.info(f"Minor checkpoint loaded - Skipping to {self.chkpt.minor}...")
                asset_archives = self.skip_to(asset_archives, self.chkpt.minor)
            self.log.info(f"Processing {len(asset_archives)} asset archives")
            for archive in asset_archives:
                cname = archive.split(".")[0]
                self.log.info(f"Begin processing asset: {cname}")
                Archive.restore(f"{label_path}/{archive}")
                if shutil.os.path.isdir(f"{label_path}/{cname}"):
                    shutil.move(f"{label_path}/{archive}", f"{label_path}/{archive}.old")
                else:
                    self.log.error(f"Archive restore failed for {label_path}/{archive}")
                    self.log.info("Moving archive to recovery.")
                    shutil.move(f"{label_path}/{archive}", self.cfg.recovery)
                    self.log.info("Proceeding to next asset.")
                    continue
                if self.inventory.add_asset(f"{label_path}/{cname}"):
                    asset_id = self.inventory.db.asset_id(cname)
                    self.log.info(f"Successful inventory of {cname} as ID {asset_id}.")
                else:
                    self.log.error(f"Inventory of {cname} failed without an exception.")
                    self.log.info("Moving asset dir and original archive to recovery.")
                    shutil.move(f"{label_path}/{archive}.old", self.cfg.recovery)
                    shutil.move(f"{label_path}/{cname}", self.cfg.recovery)
                    self.log.info("Proceeding to next asset.")
                    continue
                if Archive.archive(f"{label_path}/{cname}"):
                    self.log.info(f"Successful archive of {cname} as {archive}.")
                else:
                    self.log.error(f"Archive failed after successful inventory: {cname}")
                    self.log.info("Moving asset dir and original archive to recovery.")
                    shutil.move(f"{label_path}/{archive}.old", self.cfg.recovery)
                    shutil.move(f"{label_path}/{cname}", self.cfg.recovery)
                    self.log.info("Proceeding to next asset.")
                    continue
                shutil.rmtree(f"{label_path}/{cname}")
                _digest = self.inventory.digest(f"{label_path}/{archive}")
                if self.inventory.db.update_asset_digest(asset_id, _digest):
                    self.log.debug(f"Hash digest updated for asset ID {asset_id}.")
                else:
                    self.log.error(f"Unable to update asset hash digest for {archive}")
                    self.log.info("Proceeding.")
                self.log.info(f"Asset processing completed: {cname}")
                self.chkpt.set(minor=archive)
                shutil.os.remove(f"{label_path}/{archive}.old")
            self.log.info(f"Label processing completed: {label_dir}.")
            self.chkpt.set(major=label_dir)
        self.log.info("Build complete. Purging checkpoint.")
        self.chkpt.purge()
        return True
    
    def skip_to(self, original: List[object], target: object) -> List[object]:
        for _i, _o in enumerate(original):
            if _o == target:
                return original[_i+1:]



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
