#!/usr/bin/python3

import shutil
from typing import List

from apps.ConfiguredApp import App
from util.FileUtility import FileUtility as files
from util.FileUtility import Archive, Inventory
from util import Logs
from util.NameUtility import Validate


class Build(App):
    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)
        self.log = Logs.initialize_logging(self.cfg)
        self.inventory = Inventory(self.cfg, self.log)
        self.log.info("Catalog Build application initialized.")
    
    def update_labels(self) -> bool:
        names_in_dirs = files.collect_labels(self.cfg.root)
        if not all([len(names_in_dirs[_k]) <= 1
                    for _k in names_in_dirs.keys()]):
            raise RuntimeError("Normalize catalog prior to updating database.")
        known_label_dirs = self.inventory.db.all_label_dirs()
        for label_dir in names_in_dirs.keys():
            if not names_in_dirs[label_dir]:
                continue
            if not label_dir in known_label_dirs:
                label_name = names_in_dirs[label_dir][0]
                self.inventory.db.new_label(label_name, label_dir)
                self.log.info(f"New label inserted: {label_name}")
        return True
    
    def update_label_assets(self, label_dir: str) -> bool:
        label_id = self.inventory.db.label_id_by_dirname(label_dir)
        found_assets = files.get_canonical_assets(f"{self.cfg.root}/{label_dir}")
        known_assets = self.inventory.db.all_assets_by_label(label_id)
        for asset in found_assets:
            cname = asset.replace(".rar", "")
            if not cname in known_assets:
                self.inventory.db.new_asset(cname, label_id, finalize=True)
                self.log.info(f"New asset inserted: {label_dir}/{cname}")
        return True

    def run_simple_update(self) -> bool:
        self.log.info("Updating labels in catalog.")
        self.update_labels()
        label_dirs = self.inventory.db.all_label_dirs()
        for _dir in label_dirs:
            self.log.info(f"Updating assets in label directory: {_dir}")
            self.update_label_assets(_dir)
        self.log.info("Labels and assets updated but not inventoried.")
        return True





"""
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
"""