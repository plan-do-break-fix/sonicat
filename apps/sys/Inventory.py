#!/usr/bin/python3

from contextlib import closing
import re
import shutil
from yaml import load, SafeLoader

from apps.App import AppConfig, SimpleApp
from util.NameUtility import NameUtility

from typing import Dict, List, Tuple


class Inventory(SimpleApp):
    
    def __init__(self, sonicat_path) -> None:
        config = AppConfig(sonicat_path, "inventory")
        super().__init__(config)
        self.load_catalog_replicas()
        self.cln = Cleanse(f"{self.cfg.sonicat_path}")

    def run_cycle(self, task: Dict = {}) -> List[Dict]:
        if not task:
            pass # TODO - sleep/wait
        asset_path = task["args"]["data_path"]
        if not self.precheck(asset_path):
            return False # TODO  Handling failure
        self.remove_blacklisted(asset_path)
        self.fix_file_exts(asset_path)
        results = self.inventory_asset(asset_path)
        task["results"] = results
        return [task,]
    
    def precheck(self, catalog, path: str) -> bool:
        if not shutil.os.path.isdir(path):
            return False
        cname = path.split("/")[-1]
        if not NameUtility.name_is_canonical(cname):
            return False
        if self.replicas[catalog].asset_exists(cname):
            return False
        return True

    def inventory_asset(self, asset_path):
        asset_data = {
            "label": "",
            "cname": asset_path.split("/")[-1],
            "managed": 1
        }
        file_data = self.survey_asset_files(asset_path)
        return [
            {"payload_type": "asset_data", "payload": asset_data},
            {"payload_type": "file_data", "payload": file_data}
        ]

    def remove_blacklisted(self, path: str) -> bool:
        ban_dirs, ban_files = self.cln.ban_list(path)
        map(shutil.rmtree, ban_dirs)
        map(shutil.os.remove, ban_files)
        return True

    def fix_file_exts(self, path: str) -> bool:
        file_paths = self.cln.file_exts(path)
        for _pair in file_paths:
            shutil.move(_pair[0], _pair[1])
        return True

    def survey_asset_files(self, path: str) -> Dict[str, Dict[str, str]]:
        file_data = {}
        for _path in shutil.os.walk(path):
            for basename in _path[2]:
                dirname = _path[0].replace(path, "")
                fpath = "/".join([_path[0], basename])
                file_data[f"{dirname}/{basename}"] = {
                    "basename": basename,
                    "dirname": dirname,
                    "size": shutil.os.path.getsize(fpath),
                    "filetype": NameUtility.file_extension(basename)
                }
        return file_data


class Cleanse:
    """
    Returns list(s) of necessary normalization FS changes.
    All methods are side-effect free.
    """

    def __init__(self, sonicat_path: str) -> None:
        with closing(open(f"{sonicat_path}/config/file-blacklist.yaml", "r")) as _f:
            data = load(_f.read(), SafeLoader)
        self.ban = {"fname": data["basename"],
                    "dname": data["dirname"]}

    def ban_list(self, path) -> Tuple[List[str]]:
        ban_dirs, ban_files = [], []
        for _r, _dlist, _flist in shutil.os.walk(path):
            for _dname in _dlist:
                if _dname.lower() in self.ban["dname"]:
                    ban_dirs.append(f"{_r}/{_dname}")
            for _fname in _flist:
                if _fname.lower() in self.ban["fname"]:
                    ban_files.append(f"{_r}/{_fname}")
        ban_dirs = [_l.replace("//", "/") for _l in ban_dirs]
        ban_files = [_l.replace("//", "/") for _l in ban_files]
        for _d in ban_dirs:
            ban_files = [_f for _f in ban_files if not _f.startswith(_d)]
        return (ban_dirs, ban_files)

    def file_exts(self, path) -> List[Tuple[str]]:
        file_paths = []
        for _r, _, _flist in shutil.os.walk(path):
            for _fname in _flist:
                result = re.search(r"\.[A-Z]+$", _fname)
                if not result:
                    continue
                _fixed = _fname.replace(result.group(), result.group().lower())
                file_paths.append((f"{_r}/{_fname}", f"{_r}/{_fixed}"))
        return file_paths
 
