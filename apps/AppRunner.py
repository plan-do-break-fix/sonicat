

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

    def run_cycle(self):
        task_result = self.app.run_task(self.next_task())
        return self.route_task(task_result)

    def shutdown(self):
        pass

    def next_task(self) -> Dict:
        task = self.command.dequeue(self.app.cfg.app_name)
        if not task:
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
        if routing_app_name in ("app_data", "file_mover"):
            return "tasks"
        elif any([routing_app_type in ("analysis", "metadata", "tokens"),
                  routing_app_name in ("inventory")]
                  ):
            return "app_data"
        else:
            return task["app_name"]



 