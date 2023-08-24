#!/usr/bin/python3

from apps.ConfiguredApp import App
from contextlib import closing
import json
from util.FileUtility import FileUtility as files
from util.FileUtility import Inventory
from util import Logs


class Catalog(App):
    pass


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
                self.inventory.db.new_asset(cname, label_id, finalize=False)
                asset_id = self.inventory.db.asset_id(cname)
                json_path = f"{self.cfg.data}/csv-survey/{label_dir}/{cname}.json"
                self.add_asset_files_from_survey_json(asset_id, json_path)
                self.inventory.db.commit()
                self.log.info(f"New asset inserted: {label_dir}/{cname}")
        return True

    def read_asset_file_data_json(self, path: str) -> bool:
        with closing(open(path, "r")) as _f:
            return json.load(_f)

    def add_asset_files_from_survey_json(self, asset_id: int, 
                                               json_path: str
                                               ) -> bool:
        filetype_cache = {}
        file_data = self.read_asset_file_data_json(json_path)
        for file_path in file_data.keys():
            if file_data[file_path]["dirname"].startswith("/"):
                _dirname = file_data[file_path]["dirname"][1:]
            else:
                _dirname = file_data[file_path]["dirname"]
            if file_data[file_path]["basename"].startswith("/"):
                _basename = file_data[file_path]["basename"][1:]
            else:
                _basename = file_data[file_path]["basename"]
            ext = file_data[file_path]["filetype"]
            if not ext in filetype_cache.keys():
                if not self.inventory.db.filetype_exists(ext):
                    self.inventory.db.new_filetype(ext)
                filetype_cache[ext] = self.inventory.db.filetype_id(ext)
            self.inventory.db.new_file(asset=asset_id, 
                                       basename=_basename,
                                       dirname=_dirname,
                                       size=file_data[file_path]["size"],
                                       filetype=filetype_cache[ext],
                                       finalize=False)
        return True
            

    def run(self) -> bool:
        self.log.info("Updating labels in catalog.")
        self.update_labels()
        label_dirs = self.inventory.db.all_label_dirs()
        for _dir in label_dirs:
            self.log.info(f"Updating assets in label directory: {_dir}")
            self.update_label_assets(_dir)
        #self.log.info("Labels and assets updated but not inventoried.")
        return True



