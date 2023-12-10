#!/usr/bin/python3

import shutil

from apps.App import AppConfig, SimpleApp
from interfaces.database.Catalog import WriteInterface
from util.FileUtility import Archive, Cleanse, FileUtility
from util.NameUtility import NameUtility



class Inventory(SimpleApp):

    def __init__(self, sonicat_path: str) -> None:
        config = AppConfig(sonicat_path, "intake")
        super().__init__(config)
        self.cln = Cleanse(f"{sonicat_path}/config/file-blacklist.yaml")

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


  # Managed Asset Intake
    def managed_intake(self, cname: str) -> bool:
        path = f"{self.intake}/{cname}"
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
        if not self.writable[self.catalog_name].new_asset(cname, label_id, managed=1):
            self.log.error(f"Intake failure during asset insertion")
            return False
        asset_id = self.writable[self.catalog_name].asset_id(cname)
        if not self.insert_asset_files(asset_id, file_data):
            self.log.error(f"Intake failure during asset file insertion")
            return False
        label_dir = NameUtility.label_dir_from_cname(cname)
        if not self.archive_managed_asset(path, cname, label_dir):
            self.log.error(f"Intake failure during archiving of asset")
            return False
        self.writable[self.catalog_name].commit()
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
        if self.writable[self.catalog_name].asset_exists(cname):
            self.log.warning("Validation Failure - Asset already exists in catalog")
            return False
        self.log.debug("END precheck validation: Success")
        return True
    
    def archive_managed_asset(self, target: str, cname: str, label_dir: str) -> bool:
        self.log.debug("BEGIN archiving asset")
        target = f"{self.intake}/{cname}"
        store_path = f"{self.managed}/{label_dir}/"
        if not shutil.os.path.exists(store_path):
            shutil.os.mkdir(store_path)
        shutil.move(target, f"{self.temp}/")
        Archive.archive(f"{self.temp}/{cname}")
        self.log.debug("Archive created successfully")
        shutil.move(f"{self.temp}/{cname}.rar", store_path)
        if not shutil.os.path.isfile(f"{store_path}/{cname}.rar"):
            raise RuntimeWarning
        self.log.debug("Archive moved to label directory")
        shutil.rmtree(f"{self.temp}/{cname}")
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
        self.writable[self.catalog_name].new_asset(cname, label_id, managed=0)
        if file_data:
            asset_id = self.writable[self.catalog_name].asset_id(cname)
            if not self.insert_asset_files(asset_id, file_data):
                self.log.error(f"Intake failure during file data insertion")
                return False
        else:
            self.log.debug("Inserting asset without files")
        self.writable[self.catalog_name].commit()
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
        if not self.writable[self.catalog_name].label_exists(label):
            label_dir = NameUtility.label_dir_from_cname(cname)
            self.writable[self.catalog_name].new_label(label, label_dir)
            self.log.info(f"New label inserted: {label}")
        return self.writable[self.catalog_name].label_id_by_name(label)
    
    def filetype_id_with_insert(self, ext: str) -> str:
        if not self.writable[self.catalog_name].filetype_exists(ext):
            self.writable[self.catalog_name].new_filetype(ext)
            self.log.info(f"New filetype inserted: {ext}")
        return self.writable[self.catalog_name].filetype_id(ext)

    def insert_asset_files(self, asset_id: str, file_data: dict) -> bool:
        filetype_cache = {}
        for _k in file_data.keys():
            _f = file_data[_k]
            _ext = _f["filetype"]
            if _ext:
                if not _ext in filetype_cache.keys():
                    filetype_cache[_ext] = self.filetype_id_with_insert(_ext)
                filetype_id = filetype_cache[_ext]
            else:
                filetype_id = ""
            self.writable[self.catalog_name].new_file(asset_id,
                             _f["basename"],
                             _f["dirname"],
                             _f["size"],
                             filetype_id)
            self.log.debug(f"New file inserted: {_f['dirname']}/{_f['basename']}")
        return True

  # Purge
    def purge_file_from_asset(self) -> bool:
        file_id = ""
        self.writable[self.catalog_name].remove_file(file_id)

    def purge_asset(self, cname) -> bool:
        asset_id = self.writable[self.catalog_name].asset_id(cname)
        if self.writable[self.catalog_name].asset_is_managed(asset_id):
            #purge FS
            label_dir = NameUtility.label_dir_from_cname(cname)
            asset_path = f"{self.managed}/{label_dir}/{cname}.rar"
            shutil.move(asset_path, f"{self.temp}/")
        # purge DB
        pass

  # Validate
    def check_database(self) -> bool:
        if not all([self.check_coverage()
                   ]):
            return False
        return True
        
    def check_coverage(self) -> bool:
        found_labels = list(FileUtility.collect_labels(self.managed).keys())
        expected_labels = self.writable[self.catalog_name].all_label_dirs()
        if not self.crosscheck_lists(expected_labels, found_labels):
            return False
        for _label in found_labels:
            found_assets = [_a.replace(".rar", "")
                            for _a in FileUtility.get_canonical_assets(
                            f"{self.managed}/{_label}/")
                            ]
            label_id = self.writable[self.catalog_name].label_id_by_dirname(_label)
            all_asset_data = self.writable[self.catalog_name].asset_data_by_label(label_id)
            expected_assets = [_a["name"] for _a in all_asset_data]
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


