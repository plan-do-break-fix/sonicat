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
        if not shutil.os.path.isfile(f"{store_path}/{cname}.rar"):
            raise RuntimeWarning
        shutil.rmtree(f"{self.cfg.temp}/{cname}")
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
        return True

    def unmanaged_batch_intake(self, path: str, survey=False) -> None:
        assets, file_data = FileUtility.get_canonical_assets(path), {}
        for cname in assets:
            if survey:
                _path = f"{path}/{cname}"
                file_data = FileUtility.survey_asset_files(_path)
            self.unmanaged_intake(cname, file_data)
    
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
    