

from time import time

from apps.App import AppConfig, SimpleApp
from util.FileUtility import FileUtility
from util.NameUtility import NameUtility

from typing import Dict, List, Tuple, Union


class PendingTaskCache:

    def __init__(self) -> None:
        self.tasks = {}

    def check_in(self, task_id: Dict) -> Dict:
        if not task_id in self.tasks.keys():
            return {}
        return self.tasks.pop(task_id)
        

class TaskMaker:

    def task_id(self) -> int:
        return str(int(time() * 10000000))
    
    def make(self, app_name, action, args={}):
        return {"id": self.task_id(),
                "app_name": app_name,
                "action": action,
                "args": args,
                "results": []
                }
    


class Tasks(SimpleApp):

    """
    Naming conventions:

    task_set: List[Dict]
    A <list> of tasks where:
      - <list>[i] is to be held pending return of <list>[i-1]
      - <list>[0] is to be routed immediately
      Suggested Consumption: 
        while len(task_set) > 1:
            task_set.pop()
        return task_set[0]   


    """

    def __init__(self, config: AppConfig) -> None:
        super().__init__(config, "tasks")
        self.load_catalog_replicas()
        self.load_appdata_replicas()
        self.pending = PendingTaskCache()
        self.tm = TaskMaker()

    def process_inbound(self, task: Dict) -> List[Dict]:
        if all([task["result"]["success"] == True,
                task["id"] in self.pending.keys()
                ]):
            return self.pending.check_in(task["id"])
        return []

    def process_command(self, task: Dict) -> List[Dict]:
        command_tasks = []
        return command_tasks

    def run_cycle(self, task={}) -> bool:
        if task:
            if task["app_name"] == "command_bridge":
                return self.process_command(task)
            else:
                return self.process_inbound(task)
        else:
            return self.make_tasks()
    
    def make_tasks(self, catalogs=[]) -> List:
        catalogs = catalogs if catalogs else self.cfg.catalog_names()
        tasks = []
        for catalog in catalogs:
            tasks += self.make_tasks_by_asset(
                     self.task_lists_by_asset(
                     self.asset_lists_by_task(
                     catalog
                     )))
        return tasks

    def filter_lists(self, all_entities: List, subsets: List[List]) -> List:
        """
        Returns items in <all_entities> and not in any list in <subsets>.
        """
        return [_i for _i in all_entities
                if all([_i not in subset for subset in subsets])]

    def asset_lists_by_task(self, catalog) -> Dict[str, Dict[str, List[str]]]:
        """
        Returns dictionary {<app>: <action>: List[asset_id]}
        That is, asset IDs in <catalog> not yet processed by <app>.<action>.
        """
        all_assets = self.replicas[catalog].all_asset_ids(catalog)
        tasks_to_run = self.cfg.catalog_cfg[catalog]["tasks"]
        asset_lists = {}
        for _type in tasks_to_run.keys():
            for _app in tasks_to_run[_type]:
                asset_lists[_app] = {}
                _completed = self.replicas[_app].get_completed()
                _failed = self.replicas
                for _action in tasks_to_run[_type][_app]["actions"]:
                    asset_lists[_app][_action] = \
                            self.filter_lists(all_assets, [_completed, _failed])
        return asset_lists
            
    def task_lists_by_asset(self, assets_by_task) -> Dict[str, List[Tuple[str]]]:
        """
        Returns dictionary {<asset_id>: <app>: List[<action>]}
        That is, <app>.<action>s to be run on each given asset ID in <catalog>.
        """
        task_lists = {}
        for _app in assets_by_task.keys():
            for _action in assets_by_task[_app].keys():
                for _id in assets_by_task[_app][_action]:
                    if _id not in task_lists.keys():
                        task_lists[_id] = []
                    task_lists[_id].append((_app, _action))
        return task_lists

    def make_tasks_by_asset(self, catalog, tasks_by_asset, threshold=0):
        tasks = []
        cleanup_task = {}
        for _app in ["librosa", "cue_parser"]:
            if any([_app in app_action for app_action in tasks_by_asset[asset_id]]):
                rar_path = f"{self.cfg.catalog_cfg[catalog]['managed']}/{label_dir}/{cname}.rar"
                data_path = f"{self.cfg.temp_path}/{cname}"
                restore_args = {"from": rar_path,
                                "to": data_path}
                tasks.append(self.tm.make("file_mover", "restore", restore_args))
                cleanup_task = self.tm.make("file_mover", "remove", {"data_path": data_path})
                break

        for asset_id in tasks_by_asset.keys():
            cname = self.replicas[catalog.asset_data(asset_id)]
            label_dir = NameUtility.label_dir_from_cname(cname)
            task_args = {"catalog": catalog,
                         "asset_id": asset_id,
                         "cname": cname}
            
            for app_action in tasks_by_asset[asset_id]:
                _args = self.add_args(app_action[0], app_action[1], task_args)
                if not _args:
                    break
                tasks.append(self.tm.make(app_action[0], app_action[1], _args))

            if threshold and len(tasks) >= threshold:
                break
        if cleanup_task:
            tasks.append(cleanup_task)
        tasks.reverse()
        return tasks

    def add_args(self, app, action, task_args) -> Dict:
        catalog, asset_id = task_args["catalog"], task_args["asset_id"]
        if app == "librosa":
            return self.add_args_file_data(catalog, asset_id, "wav", task_args)
        elif app == "cue_parser":
            return self.add_args_file_data(catalog, asset_id, "cue", task_args)
        return task_args
    
    def add_args_file_data(self, catalog, asset_id, file_ext, task_args) -> Dict:
        filetype_id = self.replicas[catalog].cached_filetype_id(file_ext)
        file_data = self.replicas[catalog].file_data_by_asset_and_type(asset_id, filetype_id)
        task_args["file_data"] = file_data
        return task_args

    def add_args_file_paths(self, catalog, asset_id, file_ext, task_args) -> Dict:
        filetype_id = self.replicas[catalog].cached_filetype_id(file_ext)
        file_ids = self.replicas[catalog].file_ids_by_asset_and_type(asset_id, filetype_id)
        file_paths = [self.replicas[catalog].file_path(_id) for _id in file_ids]
        if not file_paths:
            return {}
        task_args["file_paths"] = [_t for _t in zip(file_ids, file_paths)]
        return task_args

  



# BASEMENT
"""
    def make_tasks_by_catalog(self, catalog) -> List[Dict]:
        tasks, tasks_to_run = [], self.cfg.catalog_cfg[catalog]["tasks"]
        tasks += self.make_intake_tasks(catalog)
        if not tasks and not tasks_to_run:
            return tasks


        # TODO asset id filtering

        if "api_metadata" in tasks_to_run.keys():
            tasks += self.make_api_metadata_tasks(catalog)
        if "librosa" in tasks_to_run.keys():
            tasks += self.make_librosa_tasks(catalog)
        if "local_parser" in tasks_to_run.keys():
            tasks += self.local_parser_tasks(catalog)
        if "web_metadata" in tasks_to_run.keys():
            tasks += self.make_web_metadata_tasks(catalog)
        return tasks
    
    def make_intake_tasks(self, catalog) -> List[Dict]:
        intake_path = self.cfg.catalog_cfg[catalog]["paths"]["intake"]
        assets_to_intake = FileUtility.get_canonical_assets(intake_path)
        return [self.asset_intake_task_set(catalog, asset_path) 
                for asset_path in assets_to_intake]

    def intake_task_set(self, catalog, asset_path) -> Dict:
        cname = NameUtility.cname_from_asset_path(asset_path)
        label_dir = NameUtility.label_dir_from_cname(cname)
        managed_path = self.cfg.catalog_cfg[catalog]["paths"]["managed"]
        archive_path = f"{managed_path}/{label_dir}/{cname}.rar"
        file_mover_task_args = {"from": asset_path,
                                "to": self.cfg.temp_path}
        file_mover_task = self.tm.make("file_mover", "move", file_mover_task_args)
        inventory_task_args = {"data_path": asset_path,
                               "catalog": catalog,
                               "cname": cname,
                               "label_dir": label_dir}
        inventory_task = self.tm.make("inventory", "inventory", inventory_task_args)
        archive_task_args = {"from": f"{self.cfg.temp_path}/{cname}", 
                             "to": archive_path}
        archive_task = self.tm.make("file_mover", "archive", archive_task_args)
        self.pending[file_mover_task["id"]] = [inventory_task,]
        self.pending[inventory_task["id"]] = [archive_task,]
        return file_mover_task
    
    def make_librosa_tasks(self, catalog, all_asset_ids) -> List[Dict]:
        tasks = []
        tasks_to_run = self.cfg.catalog_cfg[catalog]["tasks"]["librosa"]
        if "basic" in tasks_to_run:
            tasks += self.make_librosa_basic_tasks(catalog)
        return tasks

    def make_librosa_basic_tasks(self, catalog) -> List[Dict]:
        # TODO
        completed_asset_ids = self.
        # TODO
        assets_to_process = []
        return [self.librosa_basic_task_set(catalog, asset_id)
                for asset_id in assets_to_process]

    def librosa_basic_task_set(self, catalog, asset_id) -> Dict:
        cname = self.replicas[catalog].asset_data(asset_id)["name"]
        label_dir = NameUtility.label_dir_from_cname(cname)
        managed_path = self.cfg.catalog_cfg[catalog]["paths"]["managed"]
        archive_path = f"{managed_path}/{label_dir}/{cname}.rar"
        archive_task_args = {"from": archive_path,
                             "to": self.cfg.temp_path}
        archive_task = self.tm.make("file_mover", "restore", archive_task_args)
        analysis_task_args = {"path": f"{self.cfg.temp_path}/{cname}",
                              "catalog": catalog,
                              "asset_id": asset_id,
                              "cname": cname}
        analysis_task = self.tm.make("librosa", "basic", analysis_task_args)
        clean_up_task_args = {"path": f"{self.cfg.temp_path}/{cname}"}
        clean_up_task = self.tm.make("file_move", "remove", clean_up_task_args)
        self.pending[archive_task["id"]] = analysis_task
        self.pending[analysis_task["id"]] = clean_up_task
        return archive_task

    def make_local_parser_tasks(self, catalog) -> List[Dict]:
        tasks = []
        tasks_to_run = self.cfg.catalog_cfg[catalog]["tasks"]["local_parser"]
        if "path_parser" in tasks_to_run:
            tasks += self.make_path_parser_tasks(catalog)
        if "cue_parser" in tasks_to_run:
            tasks += self.make_cue_parser_tasks(catalog)
        return tasks

    def make_path_parser_tasks(self, catalog) -> List[Dict]:
        pass

    def path_parser_task_sets(self, catalog) -> Dict:
        pass

    def make_api_metadata_tasks(self, catalog) -> List[Dict]:
        tasks = []
        tasks_to_run = self.cfg.catalog_cfg[catalog]["tasks"]["api_metadata"]
        if "discogs" in tasks_to_run:
            tasks += self.make_discogs_api_tasks(catalog)
        if "lastfm" in tasks_to_run:
            tasks += self.make_lastfm_api_tasks(catalog)
        return 
    
    def make_web_metadata_tasks(self, catalog) -> List[Dict]:
        tasks = []
        tasks_to_run = self.cfg.catalog_cfg[catalog]["tasks"]["web_metadata"]
        if "search" in tasks_to_run:
            tasks += self.web_search_tasks(catalog)
        html_harvester_tasks = []
        html_parser_tasks = []
        return html_harvester_tasks + html_parser_tasks
    
    def web_search_tasks(self, catalog) -> List[Dict]:
        # TODO
        unsearched_assets = []
        
    
    def html_harvester_tasks(self, catalog) -> List[Dict]:
        # TODO
        urls_to_harvest = []
        return [self.tm.make("html_harvester", "get", {"url": _url})
                for _url in urls_to_harvest]

    def html_parser_tasks(self, catalog) -> List[Dict]:
        # TODO
        unparsed_html = []
        return [self.tm.make("html_parser", "parse", {""})]

"""