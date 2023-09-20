#!/usr/bin/python3

import shutil
from typing import List

from apps.ConfiguredApp import App
from interfaces.Catalog import CatalogInterface
from util.FileUtility import Archive, FileUtility
from util.NameUtility import NameUtility
from util import Logs



class Catalog(App):

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        self.db_path = f"{self.cfg.data}/{self.cfg.moniker}.sqlite"
        self.db = CatalogInterface(self.db_path)

  # Managed Asset Intake
    def managed_intake(self, path: str):
        if not self.managed_intake_precheck(path):
            raise ValueError
        cname = path.split("/")[-1]
        label_id = self.label_id_with_insert(cname)
        file_data = FileUtility.survey_asset_files(path)
        self.db.new_asset(cname, label_id, managed=1)
        asset_id = self.db.asset_id(cname)
        self.insert_asset_files(asset_id, file_data) 
        self.db.commit()
        label_dir = NameUtility.label_dir_from_cname(cname)
        self.archive_managed_asset(path, label_dir)
        return True

    def managed_intake_precheck(self, path: str) -> bool:
        if not shutil.os.path.isdir(path):
            return False
        cname = path.split("/")[-1]
        if not NameUtility.name_is_canonical(cname):
            return False
        if self.db.asset_exists(cname):
            return False
        return True
    
    def archive_managed_asset(self, cname: str, label_dir: str) -> bool:
        target = f"{self.cfg.intake}/{cname}"
        store_path = f"{self.cfg.managed_assets}/{label_dir}/"
        if not shutil.os.path.exists(store_path):
            shutil.os.mkdir(store_path)
        FileUtility.move_asset(target, self.cfg.temp)
        Archive.archive(target)
        FileUtility.move_asset(f"{self.cfg.temp}/{cname}.rar", store_path)
        return True
    
    def managed_batch_intake(self, path: str) -> None:
        assets = FileUtility.get_canonical_assets(path)
        for asset in assets:
            asset_path = f"{path}/{asset}"
            self.managed_intake(asset_path)

  # Unmanaged Asset Intake
    def unmanaged_intake(self, cname: str, file_data={}) -> bool:
        label_id = self.label_id_with_insert(cname)
        self.db.new_asset(cname, label_id, managed=0)
        if file_data:
            asset_id = self.db.asset_id(cname)
            self.insert_asset_files(asset_id, file_data)
        self.db.commit()

    def unmanaged_batch_intake(self, path: str, file_survey=False) -> None:
        assets = FileUtility.get_canonical_assets(path)
        for asset in assets:
            if not file_survey:
                self.unmanaged_intake(asset)
            elif file_survey:
                file_data = FileUtility.survey_asset_files(path)
                self.unmanaged_intake(asset, file_data)
    
  # Intake Helpers
    def label_id_with_insert(self, cname: str) -> str:
        label = NameUtility.divide_cname(cname)[0]
        if not self.db.label_exists(label):
            label_dir = NameUtility.label_dir_from_cname(cname)
            self.db.new_label(label, label_dir)
        return self.db.label_id_by_name(label)
    
    def filetype_id_with_insert(self, ext: str) -> str:
        if not self.db.filetype_exists(ext):
            self.db.new_filetype(ext)
        return self.db.filetype_id(ext)

    def insert_asset_files(self, asset_id: str, file_data: dict) -> None:
        filetype_cache = {}
        for _k in file_data.keys():
            _f = file_data[_k]
            _ext = _f["filetype"]
            if not _ext in filetype_cache.keys():
                filetype_cache[_ext] = self.filetype_id_with_insert(_ext)
            filetype_id = filetype_cache[_ext]
            self.db.new_file(asset_id,
                             _f["basename"],
                             _f["dirname"],
                             _f["size"],
                             filetype_id)
        
  # Export
    def export_asset(self, cname: str) -> bool:
        if not self.export_precheck(cname):
            return False
        label_dir = NameUtility.label_dir_from_cname(cname)
        archive_path = f"{self.cfg.managed}/{label_dir}/{cname}.rar"
        FileUtility.copy_asset(archive_path, f"{self.cfg.temp}/")
        Archive.restore(f"{self.cfg.temp}/{cname}.rar")
        FileUtility.move_asset(f"{self.cfg.temp}/{cname}", self.cfg.export)
        return True

    def export_precheck(self, cname) -> bool:
        if not self.db.asset_is_managed(self.db.asset_id(cname)):
            return False
        return True
        
  # Purge
    def purge_file_from_asset(self) -> bool:
        file_id = ""
        self.db.remove_file(file_id)


    def purge_asset(self, cname) -> bool:
        asset_id = self.db.asset_id(cname)
        if self.db.asset_is_managed(asset_id):
            #purge FS
            label_dir = NameUtility.label_dir_from_cname(cname)
            asset_path = f"{self.cfg.managed}/{label_dir}/{cname}.rar"
            FileUtility.move_asset(asset_path, f"{self.cfg.temp}/")
        # purge DB
        pass

  # Validate
    def check_database(self) -> bool:
        return True

    def export_database(self) -> bool:
        if not self.check_database():
            raise RuntimeWarning
        FileUtility.copy_asset(self.db_path, self.cfg.export)
        return True




from contextlib import closing
import json

class Build(Catalog):

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        self.db = CatalogInterface(f"{self.cfg.data}/{self.cfg.moniker}.sqlite")

    def read_asset_survey_json(self, path: str) -> bool:
        with closing(open(path, "r")) as _f:
            return json.load(_f)
        
    def managed_intake_from_survey_json(self, json_path: str) -> bool:
        cname = json_path.split("/")[-1].replace(".json", "")
        label_id = self.label_id_with_insert(cname)
        file_data = self.read_asset_survey_json(json_path)
        self.db.new_asset(cname, label_id, managed=1)
        asset_id = self.db.asset_id(cname)
        self.insert_asset_files(asset_id, file_data)
        self.db.commit()
        return True

    def gather_json(self, path: str):
        return [f"{_r}/{_f}"
                for (_r, _, _f) in shutil.os.walk(path)
                if _f.endswith(".json")
                ]

    def run(self, path: str):
        for _j in self.gather_json(path):
            self.managed_intake_from_survey_json(_j)
    
            




'''
class Build(App):
    """
    Build allows for the creation of a catalog database for pre-existing assets
            without having to process each through the standard intake process.
    Build assumes all asserts are normalized and surveyed by AsyncSurvey.
    """
    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        self.db = CatalogInterface(f"{self.cfg.data}/{self.cfg.moniker}.sqlite")
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info("Catalog Build application initialized.")
    
    def update_labels(self) -> bool:
        names_in_dirs = FileUtility.collect_labels(self.cfg.root)
        if not all([len(names_in_dirs[_k]) <= 1
                    for _k in names_in_dirs.keys()]):
            raise RuntimeError("Normalize catalog prior to updating database.")
        known_label_dirs = self.db.all_label_dirs()
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
        found_assets = FileUtility .get_canonical_assets(f"{self.cfg.root}/{label_dir}")
        known_assets = self.inventory.db.label_assets_by_cname(label_id)
        for asset in found_assets:
            cname = asset.replace(".rar", "")
            if not cname in known_assets:
                self.inventory.db.new_asset(cname, label_id, 1, finalize=False)
                asset_id = self.inventory.db.asset_id(cname)
                json_path = f"{self.cfg.data}/csv-survey/{label_dir}/{cname}.json"
                self.add_asset_files_from_survey_json(asset_id, json_path)
                self.inventory.db.commit()
                self.log.info(f"New asset inserted: {label_dir}/{cname}")
        return True


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

'''

