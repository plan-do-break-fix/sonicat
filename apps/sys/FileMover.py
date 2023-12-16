

import shutil
import subprocess

from apps.App import AppConfig, SimpleApp


class FileMover(SimpleApp):

    def __init__(self, config: AppConfig) -> None:
        super().__init__(config, "file_mover")

    def run_cycle(self, task={}):
        if not task:
            self.idle(0)
        if not shutil.os.path.isdir(task["args"]["to_path"]):
            shutil.os.makedirs(task["args"]["to_path"])
        match task["action"]:
            case "move":
                return self.move(task["args"]["from_path"], task["args"]["to_path"])
            case "remove":
                return self.remove(task["args"]["from_path"])
            case "archive":
                return self.archive(task["args"]["from_path"], task["args"]["to_path"])
            case "restore":
                return self.restore(task["args"]["from_path"], task["args"]["to_path"])
            case _:
                pass

    def move(self, from_path, to_path) -> bool:
        result = shutil.move(from_path, to_path)
        return result == to_path
    
    def remove(self, from_path) -> bool:
        if shutil.os.path.isdir(from_path):
            shutil.rmtree(from_path)
        elif shutil.os.path.isfile(from_path):
            shutil.os.remove(from_path)
        else:
            return False
        return True

    def archive(self, from_path, to_path="") -> bool:
        Archive.archive(from_path)
        if to_path:
            self.move(f"{from_path}.rar", to_path)
        self.remove(from_path)
        return True

    def restore(self, from_path, to_path) -> bool:
        shutil.copyfile(from_path, to_path)
        Archive.restore(to_path)
        self.remove(to_path)



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
        subprocess.run(["unrar", "x", target])
        return True
 