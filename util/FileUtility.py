#!/usr/bin/python3

# File operations for sound asset files
# JDSwan 2023.05.19

from contextlib import closing
from hashlib import blake2b
from logging import Logger
import shutil
from typing import Dict, List
from yaml import load, SafeLoader

from apps.Helpers import Config
from util import Logs
from util.NameUtility import Transform, Validate

class FileUtility:

    def __init__(self, config: Config) -> None:
        self.cfg = config
        blacklist_path = f"{config.data}/file-blacklist.yaml"
        with closing(open(blacklist_path, "r")) as _f:
            self.blacklisted = load(_f.read(), SafeLoader)

    def digest(self, path: str) -> str:
        with closing(open(path, "rb")) as _f:
            return blake2b(_f.read()).hexdigest()



class Handler(FileUtility):

    def __init__(self, config: Config, log: Logger) -> None:
        super().__init__(config)
        self.log = log

    def pub_dir_from_cname(self, cname: str) -> str:
        """
        Return the name of the publisher directory associated with a canonically-names asset.
        """
        return cname.split(" - ")[0].lower().replace(" ", "_")

    def get_canonical_assets(self, dir: str) -> List[str]:
        """
        Return list of all canonically-named assets in directory.
        """
        return [_f for _f in shutil.os.listdir(dir) 
                if Validate.name_is_canonical(_f)
                ]

    def group_by_publisher(self, dir: str) -> Dict[str, str]:
        """
        Transforms list of canonically-named assets into a dictionary of (publisher_dir, list(asset_name)) key-value pairs.
        """
        grouped = {}
        for _a in self.get_canonical_assets(dir):
            if self.pub_dir_from_cname(_a) in grouped.keys():
                grouped[self.pub_dir_from_cname(_a)].append(_a)
            else:
                grouped[self.pub_dir_from_cname(_a)] = [_a]
        return grouped

    def sort_assets(self, source: str, dest: str, threshold=3) -> bool:
        """
        Moves all canonically-named assets in source path to the corresponding published directory in dest path.
        threshold controls automatic creation of publisher directories when multiple products exist for an unestablished publisher.
        """
        self.log.info("Begin sorting of intake directory.")
        grouped = self.group_by_publisher(source)
        for publisher in grouped.keys():
            pub_dir = f"{dest}/{publisher}"
            if not shutil.os.path.exists(pub_dir):
                if len(grouped[publisher]) >= threshold:
                    shutil.os.mkdir(pub_dir)
                    self.log.info(f"Publisher directory created: {publisher}")
                else: 
                    continue
            for _asset in grouped[publisher]:
                if shutil.os.path.exists(f"{pub_dir}/{_asset}"):
                    shutil.move(f"{source}{_asset}", f"{source}{_asset}.duplicate")
                    self.log.debug(f"{_asset} marked as duplicate.")
                shutil.move(f"{source}{_asset}", pub_dir)
                self.log.debug(f"{_asset} moved from intake to {publisher}.")
        return True


from interfaces.Catalog import Interface as Catalog

class Inventory(FileUtility):

    def __init__(self, config: Config, log: Logger) -> None:
        super().__init__(config)
        self.log = log
        self.db = Catalog(f"{config.data}/{config.app_name}.sqlite")
        self.log.info("Connected to Catalog database.")

    def add_asset(self, path: str, finalize=False) -> bool:
        """
        Called when a new asset is first encountered:
          Creates an asset table entry for the asset
          Creates a label table entry if necessary
          Calls self.survey_asset_files()
        """
        if not shutil.os.path.exists(path):
            raise ValueError
        _cname = path.split("/")[-1]
        label, _, _ = Transform.divide_cname(_cname)
        if not self.db.label_exists(label):
            self.db.new_label(label)
            self.log.info(f"New label inserted: {label}")
        label_id = self.db.label_id(label)
        self.db.new_asset(cname=_cname,
                          label=label_id,
                          finalize=finalize)
        asset_id = self.db.asset_id(_cname)
        self.log.debug(f"{_cname} inserted as asset ID {asset_id}")
        self.survey_asset_files(asset_id, path, finalize=False)
        self.log.info(f"Survey of {_cname} completed.")
        self.db.commit()
        self.log.info(f"Changes to database committed.")
        return True

    def survey_asset_files(self, asset_id: int, path: str, finalize=False) -> bool:
        """
        Creates a file table entry for each file in the asset.
        Creates filetype table entries as necessary.
        """
        filetype_cache = {}
        for _path in shutil.os.walk(path):
            for _basename in _path[2]:
                fpath = "/".join([_path[0], _basename])
                if _basename.lower() in self.blacklisted:
                    shutil.os.remove(fpath)
                    self.log.info(f"Blacklisted file {_basename} removed from asset.")
                    continue
                ext = _basename.split(".")[-1]
                if not ext in filetype_cache.keys(): 
                    if not self.db.filetype_exists(ext):
                        self.db.new_filetype(ext)
                        self.log.info(f"New filetype inserted: {ext}")
                    filetype_cache[ext] = self.db.filetype_id(ext)
                _digest = self.digest(fpath)
                asset_path = _path[0].replace(f"{self.cfg.root}/", "")
                self.db.new_file(asset=asset_id, 
                                 basename=_basename,
                                 dirname=asset_path,
                                 digest=_digest,
                                 size=shutil.os.path.getsize(fpath),
                                 filetype=filetype_cache[ext],
                                 finalize=finalize)
                self.log.debug(f"File {fpath} added to catalog.")


import subprocess

class Archive:

    @staticmethod
    def archive(path: str) -> bool:
        if not shutil.os.path.isdir(path):
            raise ValueError
        if path.endswith("/"):
            path = path[:-1]
        parent_dir, target = shutil.os.path.split(path)
        shutil.os.chdir(parent_dir)
        subprocess.run(["rar", "a", f"{target}.rar", target])
        return True


    @staticmethod
    def restore(path: str) -> bool:
        if not all([shutil.os.path.isfile(path),
                   path.endswith(".rar")]
                   ):
            raise ValueError
        parent_dir, target = shutil.os.path.split(path)
        shutil.os.chdir(parent_dir)
        subprocess.run(["unrar", "x", target])
        return True
