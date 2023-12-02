
from contextlib import closing
from yaml import load, SafeLoader

from apps.ConfiguredApp import SimpleApp

from typing import Tuple


class ScraperApp(SimpleApp):

    def __init__(self, sonicat_path: str, app_name: str) -> None:
        super().__init__(sonicat_path, "metadata", app_name)
        self.load_catalog_replicas()
    
    def get_credentials(self, secrets_key: str) -> Tuple[str]:
        with closing(open(f"{self.cfg['sonicat_path']}/config/secrets.yaml", "r")) as _f:
            secret = load(_f.read(), SafeLoader)
        return (secret[secrets_key]["uname"], secret[secrets_key]["passwd"])

    def run(self):
        pass