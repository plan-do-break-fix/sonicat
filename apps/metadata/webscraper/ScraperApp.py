

from apps.ConfiguredApp import SimpleApp
from interfaces.database.Catalog import CatalogInterface


class ScraperApp(SimpleApp):

    def __init__(self, sonicat_path: str, app_name: str) -> None:
        super().__init__(sonicat_path, "metadata", app_name)
        self.load_catalog_replicas()
    
    def run(self):
        pass