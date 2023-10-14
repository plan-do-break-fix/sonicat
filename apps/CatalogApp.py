#!/usr/bin/python3

import shutil
from typing import List

from apps.ConfiguredApp import App
from interfaces.Catalog import CatalogInterface
from util.FileUtility import Archive, Cleanse, FileUtility
from util.NameUtility import NameUtility
from util import Logs



class Catalog(App):

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        self.cfg.log += "/catalog"
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"BEGIN {self.cfg.name} application initialization")
        self.cln = Cleanse(f"{sonicat_path}/config/file-blacklist.yaml")
        if shutil.os.path.exists(self.cfg.temp):
            shutil.rmtree(self.cfg.temp)
        shutil.os.mkdir(self.cfg.temp)   #TODO This probably should be mkdirs
        self.db_path = f"{self.cfg.data}/catalog/{self.cfg.name}.sqlite"
        self.db = CatalogInterface(self.db_path)
        self.log.debug(f"Connected to database {self.db_path}")
        self.log.info(f"END application initialization: Success")

  # Managed Asset Intake
    def managed_intake(self, cname: str) -> bool:
        path = f"{self.cfg.intake}/{cname}"
        self.log.info(f"BEGIN managed intake of {path}")
        if not self.managed_intake_precheck(path):
            self.log.error(f"Intake Aborted - Precheck Validation Failure")
            return False
        ban_dirs, ban_files = self.cln.ban_list(path)
        self.log.debug("Removing blacklisted files from asset:")
        for _d in ban_dirs:
            self.log.debug(f"Removing {_d}")
            shutil.rmtree(_d)
        for _f in ban_files:
            self.log.debug(f"Removing {_f}")
            shutil.os.remove(_f)
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
        if not self.archive_managed_asset(path, cname, label_dir):
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
        self.log.debug("END precheck validation: Success")
        return True
    
    def archive_managed_asset(self, target: str, cname: str, label_dir: str) -> bool:
        self.log.debug("BEGIN archiving asset")
        target = f"{self.cfg.intake}/{cname}"
        store_path = f"{self.cfg.managed}/{label_dir}/"
        if not shutil.os.path.exists(store_path):
            shutil.os.mkdir(store_path)
        shutil.move(target, self.cfg.temp)
        Archive.archive(f"{self.cfg.temp}/{cname}")
        self.log.debug("Archive created successfully")
        shutil.move(f"{self.cfg.temp}/{cname}.rar", store_path)
        if not shutil.os.path.isfile(f"{store_path}/{cname}.rar"):
            raise RuntimeWarning
        self.log.debug("Archive moved to label directory")
        shutil.rmtree(f"{self.cfg.temp}/{cname}")
        self.log.debug("Original asset files removed")
        return True
    
    def managed_batch_intake(self, path: str) -> None:
        assets = FileUtility.get_canonical_assets(path)
        self.log.info(f"{len(assets)} assets found for managed batch intake.")
        for asset in assets:
            self.managed_intake(asset)

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
            if _ext:
                if not _ext in filetype_cache.keys():
                    filetype_cache[_ext] = self.filetype_id_with_insert(_ext)
                filetype_id = filetype_cache[_ext]
                self.db.new_file(asset_id,
                                 _f["basename"],
                                 _f["dirname"],
                                 _f["size"],
                                 filetype=filetype_id)
            elif not _ext:
                self.db.new_extensionless_file(asset_id,
                                               _f["basename"],
                                               _f["dirname"],
                                               _f["size"])
            self.log.debug(f"New file inserted: {_f['dirname']}/{_f['basename']}")
        return True
 
    def export_database(self, out_path) -> bool:
        if not self.check_database():
            raise RuntimeWarning
        out_path = self.cfg.export if not out_path else out_path
        shutil.copyfile(self.db_path, out_path)
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
            shutil.move(asset_path, f"{self.cfg.temp}/")
        # purge DB
        pass

  # Validate
    def check_database(self) -> bool:
        if not all([self.check_coverage()
                   ]):
            return False
        return True
        
    def check_coverage(self) -> bool:
        found_labels = list(FileUtility.collect_labels(self.cfg.managed).keys())
        expected_labels = self.db.all_label_dirs()
        if not self.crosscheck_lists(expected_labels, found_labels):
            return False
        for _label in found_labels:
            found_assets = [_a.replace(".rar", "")
                            for _a in FileUtility.get_canonical_assets(
                            f"{self.cfg.managed}/{_label}/")
                            ]
            expected_assets = self.db.asset_cnames_by_label(
                                  self.db.label_id_by_dirname(_label)
                                  )
            if not self.crosscheck_lists(expected_assets, found_assets):
                return False
        return True
    
    def crosscheck_lists(self, list_a, list_b) -> bool:
        diff = list(set(list_a).symmetric_difference(set(list_b)))
        if diff == []:
            return True
        else:
            self.print_crosscheck_report(list_a, list_b)
            return False

    def print_crosscheck_report(self, list_a, list_b):
        unique_in_a = [_i for _i in list_a if _i not in list_b]
        unique_in_b = [_i for _i in list_b if _i not in list_a]
        print(f"{len(unique_in_a)} items in list 1 not found in list 2:")
        for _a in unique_in_a:
            print(f"  {_a}")
        print(f"{len(unique_in_b)} items in list 2 not found in list 1:")
        for _b in unique_in_b:
            print(f"  {_b}")


from contextlib import closing
import json
from yaml import load, SafeLoader

class Build(Catalog):

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        self.log.info("BEGIN CATALOG BUILD MODE")
        self.log.setLevel(Logs.LOOKUP["info"])
        self.log.info("Log level changed to INFO")
        self.ban = self.load_blacklist(sonicat_path)

    def load_blacklist(self, sonicat_path: str) -> dict:
        with closing(open(f"{sonicat_path}/catalog/file-blacklist.yaml", "r")) as _f:
            return load(_f.read(), SafeLoader)

    def read_asset_survey_json(self, path: str) -> bool:
        with closing(open(path, "r")) as _f:
            return json.load(_f)
        
    def clear_invalid_filetypes(self, file_data: dict) -> dict:
        for _f in file_data.keys():
            name, ext = file_data[_f]["basename"], file_data[_f]["filetype"]
            prefixes = [".", "_.", "._."]
            if any([ext == "",
                    ext == name,
                    " " in ext,
                    any([f"{_pre}{ext}" == name for _pre in prefixes])
                    ]):
                file_data[_f]["filetype"] = ""
        return file_data
        
    def managed_intake_from_survey_json(self, json_path: str) -> bool:
        cname = json_path.split("/")[-1].replace(".json", "")
        label_id = self.label_id_with_insert(cname)
        file_data = self.read_asset_survey_json(json_path)
        self.clear_invalid_filetypes(file_data)
        self.db.new_asset(cname, label_id, managed=1)
        asset_id = self.db.asset_id(cname)
        self.insert_asset_files(asset_id, file_data)
        self.db.commit()
        self.log.info(f"New asset inserted: {cname}")
        return True

    def gather_json(self, path: str):
        out = []
        for (_r, _, _fl) in shutil.os.walk(path):
            _json = [f"{_r}/{_f}" for _f in _fl if _f.endswith(".json")]
            out += _json
        return out

    def run(self, path: str):
        self.log.info(f"BEGIN database build for {self.cfg.name}")
        total, current = len(self.gather_json(path)), 0
        failed = []
        for _j in self.gather_json(path):
            current += 1
            print(f"Running job {current}/{total}")
            if not self.managed_intake_from_survey_json(_j):
                cname = _j.split("/")[-1].replace(".json", "")
                failed.append(cname)
        self.log.info("Database build complete")
        self.log.info("BEGIN database validation")
        if not self.check_database():
            self.log.error("Database validation failed")
            raise RuntimeWarning()
        self.log.info("END database validation: Success")
        self.log.info("END database build: Success")
        if failed:
            with open(f"{self.cfg.export}/noncompliant-assets.csv", "w") as _out:
                _out.write("\n".join(failed))
            self.log.info("Failed intake report exported")
        self.log.info("END CATALOG BUILD MODE")

