#!/usr/bin/python3

# File operations for sound asset files
# JDSwan 2023.09

from contextlib import closing
from hashlib import blake2b
import shutil
from typing import Dict, List, Tuple

from util.NameUtility import NameUtility



class FileUtility:

    @staticmethod
    def digest(path: str) -> str:
        with closing(open(path, "rb")) as _f:
            return blake2b(_f.read()).hexdigest()

    @staticmethod
    def get_canonical_assets(path: str) -> List[str]:
        """
        Return list of all canonically-named assets in directory.
        """
        return [_f for _f in shutil.os.listdir(path) 
                if NameUtility.name_is_canonical(_f)
                ]

    @staticmethod
    def collect_labels(path: str) -> Dict:
        label_dirs = [_d for _d in shutil.os.listdir(path)
                      if shutil.os.path.isdir(f"{path}/{_d}")
                      and not _d.startswith(".")]
        label_paths = [f"{path}/{_d}" for _d in label_dirs]
        label_names = [list(set([_d.split(" - ")[0] for _d in shutil.os.listdir(_p)])) 
                       for _p in label_paths]
        return dict(zip(label_dirs, label_names))

    @staticmethod
    def assets_grouped_by_label_dir(dir: str) -> Dict[str, str]:
        grouped = {}
        for _a in FileUtility.get_canonical_assets(dir):
            if FileUtility.pub_dir_from_cname(_a) in grouped.keys():
                grouped[FileUtility.pub_dir_from_cname(_a)].append(_a)
            else:
                grouped[FileUtility.pub_dir_from_cname(_a)] = [_a]
        return grouped
    
    @staticmethod
    def survey_asset_files(path: str) -> Dict[str, Dict[str, str]]:
        file_data = {}
        for _path in shutil.os.walk(path):
            for basename in _path[2]:
                dirname = _path[0].replace(path, "")
                fpath = "/".join([_path[0], basename])
                file_data[f"{dirname}/{basename}"] = {
                    "basename": basename,
                    "dirname": dirname,
                    "size": shutil.os.path.getsize(fpath),
                    "filetype": FileUtility.file_extension(basename)
                }
        return file_data
    
    @staticmethod
    def file_extension(fname: str) -> str:
        ext = fname.split(".")[-1]
        if any([ext == "",
                ext == fname,
                " " in ext,
                any([f"{_p}{ext}" == fname for _p in [".", "_.", "._."]])
                    ]):
            return ""
        return ext

    #@staticmethod
    #def move_asset(target: str, destination: str) -> bool:
    #    if not all([any([shutil.os.path.isfile(target),
    #                    shutil.os.path.isdir(target)]),
    #                shutil.os.path.exists(destination)]):
    #        raise ValueError
    #    shutil.move(target, destination)
    #    return True
    
    #@staticmethod
    #def copy_asset(target: str, destination: str) -> bool:
    #    if not all([any([shutil.os.path.isfile(target),
    #                    shutil.os.path.isdir(target)]),
    #                shutil.os.path.exists(destination)]):
    #        raise ValueError
    #    shutil.copyfile(target, destination)
    #    return True


import subprocess

class Archive:

    @staticmethod
    def archive(path: str) -> bool:
        """
        Create rar archive of <path> in same directory and with same basename.
        """
        if not shutil.os.path.isdir(path):
            print("Unable to find {path}")
            raise ValueError
        if path.endswith("/"):
            path = path[:-1]
        parent_dir, target = shutil.os.path.split(path)
        shutil.os.chdir(parent_dir)
        subprocess.run(["rar", "a", f"{target}.rar", target])
        return True


    @staticmethod
    def restore(path: str) -> bool:
        """
        Expand rar file at <path> in same directory and with same name as the archive.
        """
        if not all([shutil.os.path.isfile(path),
                   path.endswith(".rar")]
                   ):
            raise ValueError
        parent_dir, target = shutil.os.path.split(path)
        shutil.os.chdir(parent_dir)
        subprocess.run(["unrar", "x", target],
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT
                       )
        return True



from yaml import load, SafeLoader

class Cleanse:

    def __init__(self, blacklist_yaml: str) -> None:
        with closing(open(blacklist_yaml, "r")) as _f:
            data = load(_f.read(), SafeLoader)
        self.ban = {"fname": data["basename"],
                    "dname": data["dirname"]}
        

    def ban_list(self, path) -> Tuple[str]:
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


    def remediate_file_ext(self, ext: str) -> str:
        return ext.lower() if ext == ext.upper() else ext
            
