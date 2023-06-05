

import shutil
from util.FileUtility import FileUtility as files
from util.Logs import StdOutLogger
from util.NameUtility import Transform, Validate

from apps.ConfiguredApp import App
from apps.Helpers import Config

class Interactive(App):

    def __init__(self, config_path):
        super().__init__(config_path)

    def homogenize_label_dirs(self):
        names_in_dirs = files.collect_labels(self.cfg.root)
        for label_dir in names_in_dirs.keys():
            if len(names_in_dirs[label_dir]) > 1:
                print(f"Multiple labels names found in {label_dir}.")
                print("Select correct label name:")
                for _i, _m in enumerate(list(names_in_dirs[label_dir])):
                    print(f"{_i}: {_m}")
                inp = int(input("> "))
                correct_label = names_in_dirs[label_dir][inp]
                for archive in files.get_canonical_assets(f"{self.cfg.root}/{label_dir}"):
                    label, title, note = Transform.divide_cname(archive)
                    if label != correct_label:
                        if note:
                            correct_name = f"{correct_label} - {title} ({note}).rar"
                        else:
                            correct_name = f"{correct_label} - {title}.rar"
                        shutil.move(f"{self.cfg.root}/{label_dir}/{archive}",
                                    f"{self.cfg.root}/{label_dir}/{correct_name}")
                print(f"Label names in directory {label_dir} have been homogenized.")
            else:
                print(f"Label directory {label_dir} contains homogenous label names.")
        print("All label directories checked. Run complete")
        return None
            

class CleanUp(App):

    def __init__(self, config_path):
        super().__init__(config_path)

    def hard_clean(self):
        for label_dir in [_d for _d in shutil.os.listdir(self.cfg.root)
                          if not _d.startswith(".")]:
            label_path = f"{self.cfg.root}/{label_dir}"
            self.remove_subdirectories(label_path)
            self.recover_noncanonical(label_path, self.cfg.recover)

    def remove_subdirectories(self, path: str):
        for dir in [_d for _d in shutil.os.listdir(path)
                     if shutil.os.path.isdir(f"{path}/{_d}")
                     and not _d.startswith(".")]:
            shutil.rmtree(f"{path}/{dir}")

    def recover_noncanonical(self, label_path: str, recover_path):
        for asset in [_a for _a in shutil.os.listdir(label_path)
                     if _a.endswith(".rar")
                     and not Validate.name_is_canonical(_a.replace(".rar", ""))
                     ]:
            shutil.move(f"{label_path}/{asset}", recover_path)
