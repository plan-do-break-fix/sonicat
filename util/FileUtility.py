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

    @staticmethod
    def digest(path: str) -> str:
        with closing(open(path, "rb")) as _f:
            return blake2b(_f.read()).hexdigest()

    @staticmethod
    def pub_dir_from_cname(cname: str) -> str:
        """
        Return the name of the publisher directory associated with a canonically-names asset.
        """
        return cname.split(" - ")[0].lower().replace(" ", "_")

    @staticmethod
    def get_canonical_assets(path: str) -> List[str]:
        """
        Return list of all canonically-named assets in directory.
        """
        return [_f for _f in shutil.os.listdir(path) 
                if Validate.name_is_canonical(_f)
                ]

    @staticmethod
    def collect_labels(dir: str):
        label_dirs = [_d for _d in shutil.os.listdir(dir)
                      if shutil.os.path.isdir(f"{dir}/{_d}")
                      and not _d.startswith(".")]
        label_paths = [f"{dir}/{_d}" for _d in label_dirs]
        label_names = [list(set([_d.split(" - ")[0] for _d in shutil.os.listdir(_p)])) 
                       for _p in label_paths]
        return dict(zip(label_dirs, label_names))


class IntakeHandler(FileUtility):

    def __init__(self, config: Config, log: Logger) -> None:
        self.cfg = config


    def assets_grouped_by_label_dir(self, dir: str) -> Dict[str, str]:
        """
        """
        grouped = {}
        for _a in FileUtility.get_canonical_assets(dir):
            if FileUtility.sub_dir_from_cname(_a) in grouped.keys():
                grouped[FileUtility.pub_dir_from_cname(_a)].append(_a)
            else:
                grouped[FileUtility.pub_dir_from_cname(_a)] = [_a]
        return grouped

    def sort_intake(self, threshold=3) -> bool:
        """
        Moves all canonically-named assets in source path to the corresponding published directory in dest path.
        threshold controls automatic creation of publisher directories when multiple products exist for an unestablished publisher.
        """
        grouped = self.assets_grouped_by_label_dir(self.cfg.intake)
        for label_dir in grouped.keys():
            label_path = f"{self.cfg.root}/{label_dir}"
            if not shutil.os.path.exists(label_path):
                if len(grouped[label_dir]) >= threshold:
                    shutil.os.mkdir(label_dir)
                else: 
                    continue
            for _asset in grouped[label_dir]:
                if shutil.os.path.exists(f"{label_path}/{_asset}"):
                    shutil.move(f"{self.cfg.intake}/{_asset}", f"{self.cfg.intake}/{_asset}.duplicate")
                else:
                    shutil.move(f"{self.cfg.intake}{_asset}", label_path)
        return True


from interfaces.Catalog import Interface as Catalog

class Inventory(FileUtility):

    def __init__(self, config: Config, log: Logger) -> None:
        self.cfg = config
        self.log = log
        self.db = Catalog(f"{config.data}/{config.app_name}.sqlite")
        blacklist_path = f"{config.config}/file-blacklist.yaml"
        with closing(open(blacklist_path, "r")) as _f:
            self.blacklisted = load(_f.read(), SafeLoader)


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
        label_id = self.db.label_id(label)
        self.db.new_asset(cname=_cname,
                          label=label_id,
                          finalize=finalize)
        asset_id = self.db.asset_id(_cname)
        self.survey_asset_files(asset_id, path, finalize=False)
        self.db.commit()
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
                    continue
                ext = _basename.split(".")[-1]
                if not ext in filetype_cache.keys(): 
                    if not self.db.filetype_exists(ext):
                        self.db.new_filetype(ext)
                    filetype_cache[ext] = self.db.filetype_id(ext)
                _digest = FileUtility.digest(fpath)
                asset_path = _path[0].replace(f"{self.cfg.root}/", "")
                self.db.new_file(asset=asset_id, 
                                 basename=_basename,
                                 dirname=asset_path,
                                 digest=_digest,
                                 size=shutil.os.path.getsize(fpath),
                                 filetype=filetype_cache[ext],
                                 finalize=finalize)


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
