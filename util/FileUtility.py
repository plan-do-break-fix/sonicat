#!/usr/bin/python3

# File operations for sound asset files
# JDSwan 2023.09

from contextlib import closing
from hashlib import blake2b
import re
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
    def export_asset_to_temp(cname, managed_path, temp_path) -> bool:
        if not shutil.os.path.isdir(temp_path):
            shutil.os.makedirs(temp_path, exist_ok=True)
        label_dir = NameUtility.label_dir_from_cname(cname)
        archive_path = f"{managed_path}/{label_dir}/{cname}.rar"
        shutil.copyfile(archive_path, f"{temp_path}/{cname}.rar")
        Archive.restore(f"{temp_path}/{cname}.rar")
        shutil.os.remove(f"{temp_path}/{cname}.rar")
        return True


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



# TODO  THIS CAN GO SOMEWHERE ELSE, like FileUtility
#  # Export



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
        #subprocess.run(["unrar", "x", target],
        #               stdout=subprocess.DEVNULL,
        #               stderr=subprocess.STDOUT
        #               )
        subprocess.run(["unrar", "x", target])
        return True




