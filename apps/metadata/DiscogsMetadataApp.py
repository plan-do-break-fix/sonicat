
from apps.ConfiguredApp import App
from interfaces.database.Catalog import CatalogInterface
from interfaces.api.Discogs import Client, Data
from util import Logs


class Discogs(App):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, moniker="releases")
        self.catalog = CatalogInterface(f"{self.cfg.data}/catalog/ReleaseCatalog-ReadReplica.sqlite")
        self.data = Data(f"{self.cfg.data}/pages/DiscogsMetadata.sqlite")
        self.cl = Client(sonicat_path)
        self.cfg.log_level = "debug"
        self.cfg.log += "/pages"
        self.cfg.name = "DiscogsMetadata"
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"Discogs Metadata Application Initialization Successful")

    def process_asset(self, asset_id):
        cname = self.catalog.asset_cname(asset_id)
        rawresult = self.cl.search(cname)
        if not rawresult:
            return False
        result = self.cl.process_result(rawresult)
        self.data.record_result("releases", asset_id, result)
        self.data.db.commit()  # Why is this here? Put this in the DB interface where it belongs
        return True


    def run(self, initial=0):
        completed = self.data.all_asset_ids_by_catalog("releases")
        _ids = self.catalog.all_asset_ids()
        _failed = self.data.all_failed_searched_by_catalog("releases")
        assets_to_search = [_id for _id in _ids if all([
            _id not in completed,
            _id not in _failed])]

        if initial:
            assets_to_search = [_id for _id in assets_to_search if int(_id) >= initial]
            assets_to_search.sort()

        for _id in assets_to_search:
            self.log.debug(f"Searching asset ID {_id} in Discogs.")
            if self.process_asset(_id):
                self.log.debug("Verified matching release data recorded.")
            else:
                self.data.record_failed_search("releases", _id)
                self.log.warning(f"No verifiable match for asset. Continuing.")
                