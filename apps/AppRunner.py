

import json
#from os import environ
#from RabbitMq import Interface as RabbitMq

# for typing
from typing import Dict
from apps.App import SimpleApp



class AppRunner:

    def __init__(self, app=None, debug=False):
        if debug:
            return None
        self.app = app

    def run(self):
        pass

    def shutdown(self):
        pass

    def next_task(self) -> Dict:
        task = self.inbound.dequeue(self.app.cfg.app_name)
        return json.loads(task) if task else {}

    def route_task(self, task: Dict):
        target = self.route_target(task)
        return self.outbound.enqueue(task, target)


    def route_target(self, task: Dict,
                           routing_app_name="",
                           routing_app_type=""
                           ) -> str:
        if not routing_app_name:
            routing_app_name = self.app.cfg.app_name
        if not routing_app_type:
            routing_app_type = self.app.cfg.app_type
        # multivariate task routing
        if all([routing_app_name == "app_data",
                task["app_name"] in ("inventory", "librosa")]):
            return "file_mover"
        if all([routing_app_name == "inventory",
                task["app_name"] == "inventory"]):
            return "app_data"
        if all([routing_app_name == "app_data",
                task["app_name"] == "inventory"]):
            return "file-mover"
        if all([routing_app_name == "file_mover",
                task["app_name"] == "inventory"]):
            return "tasks"
        # task routing by routing app name
        if routing_app_name == "tasks":
            return task["app_name"]
        if routing_app_name == "file_mover":
            return "tasks"
        
        # task routing by routing app type
        if routing_app_type in ("analysis", "metadata", "tokens"):
            return "app_data"
        return ""



 