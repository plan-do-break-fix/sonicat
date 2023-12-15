

import shutil
from util.FileUtility import FileUtility as files
from util.Logs import StdOutLogger
from util.NameUtility import NameUtility

from apps.App import App, Config

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
                    label, title, note = NameUtility.divide_cname(archive)
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
                     and not NameUtility.name_is_canonical(_a.replace(".rar", ""))
                     ]:
            shutil.move(f"{label_path}/{asset}", recover_path)


### Copied from obsolete CatalogApp class

'''
 # Validate - Maybe it's own operations class?
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


'''