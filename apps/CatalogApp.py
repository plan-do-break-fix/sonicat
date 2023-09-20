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
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"BEGIN {self.cfg.name} ({self.cfg.moniker}) application initialization")
        self.db_path = f"{self.cfg.data}/{self.cfg.moniker}.sqlite"
        self.db = CatalogInterface(self.db_path)
        self.log.debug(f"Connected to database {self.db_path}")
        self.log.info(f"END application initialization: Success")

  # Managed Asset Intake
    def managed_intake(self, path: str) -> bool:
        self.log.info(f"BEGIN managed intake of {path}")
        if not self.managed_intake_precheck(path):
            self.log.error(f"Intake Aborted - Precheck Validation Failure")
            return False
        cname = path.split("/")[-1]
        label_id = self.label_id_with_insert(cname)
        file_data = FileUtility.survey_asset_files(path)
        if not file_data:
            self.log.error(f"Intake failure during inventory of asset files")
            return False
        if not self.db.new_asset(cname, label_id, managed=1):
            self.log.error(f"Intake failure during asset insertion")
            return False
        asset_id = self.db.asset_id(cname)
        if not self.insert_asset_files(asset_id, file_data):
            self.log.error(f"Intake failure during asset file insertion")
            return False
        label_dir = NameUtility.label_dir_from_cname(cname)
        if not self.archive_managed_asset(path, label_dir):
            self.log.error(f"Intake failure during archiving of asset")
            return False
        self.db.commit()
        self.log.info("END managed intake: Success")
        return True

    def managed_intake_precheck(self, path: str) -> bool:
        self.log.debug("BEGIN precheck validation")
        if not shutil.os.path.isdir(path):
            self.log.error("Validation Failure - Target not found on FS")
            return False
        cname = path.split("/")[-1]
        if not NameUtility.name_is_canonical(cname):
            self.log.error("Validation Failure - Target is not canonically named")
            return False
        if self.db.asset_exists(cname):
            self.log.warning("Validation Failure - Asset already exists in catalog")
            return False
        self.debug("END precheck validation: Success")
        return True
    
    def archive_managed_asset(self, cname: str, label_dir: str) -> bool:
        self.log.debug("BEGIN archiving asset")
        target = f"{self.cfg.intake}/{cname}"
        store_path = f"{self.cfg.managed_assets}/{label_dir}/"
        if not shutil.os.path.exists(store_path):
            shutil.os.mkdir(store_path)
        FileUtility.move_asset(target, self.cfg.temp)
        Archive.archive(target)
        self.log.debug("Archive created successfully")
        FileUtility.move_asset(f"{self.cfg.temp}/{cname}.rar", store_path)
        if not shutil.os.path.isfile(f"{store_path}/{cname}.rar"):
            raise RuntimeWarning
        self.log.debug("Archive moved to label directory")
        shutil.rmtree(f"{self.cfg.temp}/{cname}")
        self.log.debug("Original asset files removed")
        return True
    
    def managed_batch_intake(self, path: str) -> None:
        assets = FileUtility.get_canonical_assets(path)
        for asset in assets:
            asset_path = f"{path}/{asset}"
            self.managed_intake(asset_path)

  # Unmanaged Asset Intake
    def unmanaged_intake(self, cname: str, file_data={}) -> bool:
        self.log.info(f"BEGIN unmanaged intake of {cname}")
        label_id = self.label_id_with_insert(cname)
        self.db.new_asset(cname, label_id, managed=0)
        if file_data:
            asset_id = self.db.asset_id(cname)
            if not self.insert_asset_files(asset_id, file_data):
                self.log.error(f"Intake failure during file data insertion")
                return False
        else:
            self.log.debug("Inserting asset without files")
        self.db.commit()
        self.log.info("END unmanaged intake: Success")
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
            self.log.info(f"New label inserted: {label}")
        return self.db.label_id_by_name(label)
    
    def filetype_id_with_insert(self, ext: str) -> str:
        if not self.db.filetype_exists(ext):
            self.db.new_filetype(ext)
            self.log.info(f"New filetype inserted: {ext}")
        return self.db.filetype_id(ext)

    def insert_asset_files(self, asset_id: str, file_data: dict) -> bool:
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
            self.log.debug(f"New file inserted: {_f['dirname']}/{_f['basename']}")
        return True
    
  # Export
    def export_asset(self, cname: str) -> bool:
        self.log.info(f"BEGIN export of {cname}")
        if not self.export_precheck(cname):
            self.log.error(f"Export Aborted - Precheck Validation Failed")
            return False
        label_dir = NameUtility.label_dir_from_cname(cname)
        archive_path = f"{self.cfg.managed}/{label_dir}/{cname}.rar"
        FileUtility.copy_asset(archive_path, f"{self.cfg.temp}/")
        self.log.debug(f"Archive {cname}.rar retrieved")
        Archive.restore(f"{self.cfg.temp}/{cname}.rar")
        FileUtility.move_asset(f"{self.cfg.temp}/{cname}", self.cfg.export)
        self.log.info(f"Asset exported to {self.cfg.export}")
        shutil.rmtree(f"{self.cfg.temp}/{cname}.rar")
        self.debug.log(f"Asset archive removed from {self.cfg.temp}")
        self.log.info("END asset export: Success")
        return True

    def export_precheck(self, cname) -> bool:
        if not self.db.asset_is_managed(self.db.asset_id(cname)):
            self.log.warning("Cannot export unmanaged assets")
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
        if not all(self.check_coverage()
                   ):
            return False
        return True
        
    def check_coverage(self) -> bool:
        found_labels = list(FileUtility.collect_labels(self.cfg.managed).keys())
        expected_labels = self.db.all_label_dirs()
        found_labels.sort(), expected_labels.sort()
        if not found_labels == expected_labels:
            #TODO - produce an actionable summary
            return False
        for _label in found_labels:
            found_assets = FileUtility.get_canonical_assets(f"{self.cfg.managed}/{_label}/")
            expected_assets = self.db.asset_cnames_by_label(
                                  self.db.label_id_by_dirname(_label)
                                  )
            found_assets.sort(), expected_assets.sort()
            if not found_assets == expected_assets:
                #TODO - produce an actionable summary
                return False
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
        self.log.setLevel(Logs.LOOKUP["info"])
        self.log.info("CATALOG BUILD MODE")

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
        self.log.info(f"New asset inserted: {cname}")
        return True

    def gather_json(self, path: str):
        out = []
        for (_r, _, _fl) in shutil.os.walk(path):
            _json = [_f for _f in _fl if _f.endswith(".json")]
            out += _json
        return out

    def run(self, path: str):
        self.log.info(f"BEGIN database build for {self.cfg.name}")
        total, current = len(self.gather_json(path), 0)
        for _j in self.gather_json(path):
            current += 1
            print(f"Running job {current}/{total}")
            self.managed_intake_from_survey_json(_j)
        self.log.info("Database build complete")
        self.log.info("BEGIN database validation")
        if not self.check_database():
            self.log.error("Database validation failed")
            raise ResourceWarning()
        self.log.info("END database validation: Success")
        self.log.info("END database build: Success")