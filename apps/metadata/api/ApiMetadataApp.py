

from httpcore import NetworkError
from apps.ConfiguredApp import SimpleApp
from apps.Operations import Data as DataOps
from util.NameUtility import NameUtility


class ApiMetadataApp(SimpleApp):

    def __init__(self, sonicat_path: str, api_name: str) -> None:
        super().__init__(sonicat_path, "metadata", f"{api_name}Metadata")
        self.dataops = DataOps(sonicat_path)
        self.load_catalog_replicas()
        self.api_name = api_name
        self.log.info(f"{self.app_name} Application Initialization Successful")

    def process_asset(self, catalog, asset_id) -> bool:
        tracklist = self.dataops.asset_track_list("releases", asset_id)
        try:
            result = self.validated_search(catalog, asset_id, tracklist)
        except NetworkError:
            result = False
        if not result:
            self.data.record_failed_search("releases", asset_id)
            return False
        file_ids = [_t[-1] for _t in tracklist]
        self.data.record_result("releases", asset_id, file_ids, result)
        return True

    def validated_search(self, catalog, asset_id, tracklist):
        if not tracklist:
            return False
        durations = [_t[1] for _t in tracklist]
        cname = self.replicas[catalog].asset_data(asset_id)["name"]
        label, title, year = NameUtility.divide_cname(cname)
        arg_sets = [
            {"artist": label},
            {"publisher": label},
            {},
            {"artist": label, "year": year},
            {"publisher": label, "year": year},
        ]
        if self.cl.title_has_media_type_label(title):
            trimmed_title = self.cl.drop_media_type_labels(title)
        else:
            trimmed_title = ""
        for argset in arg_sets:
            desc_str = "".join([f", {_a}" for _a in argset])
            self.log.debug(f"Searching with title{desc_str}")
            self.cl.search(title, **argset)
            if not self.cl.active_search:
                continue
            self.log.debug(f"{len(self.cl.active_search)} results returned")
            while self.cl.next_result_index < min(20, len(self.cl.active_search)):
                try:
                    result = self.cl.parse_album_result(self.cl.next_result())
                except:
                    continue
                if result:
                    if self.cl.validate_by_track_durations(durations, result.track_durations()):
                        self.log.debug("Release validated. Recording results.")
                        return result
            self.log.debug("All results failed validation.")
            if trimmed_title:
                self.log.debug(f"Searching with trimmed title{desc_str}")
                self.cl.search(title, **argset)
                self.log.debug(f"{len(self.cl.active_search)} results returned")
                while self.cl.next_result_index < min(20, len(self.cl.active_search)):
                    try:
                        result = self.cl.parse_album_result(self.cl.next_result())
                    except:
                        continue
                    if result:
                        if self.cl.validate_by_track_durations(durations, result.track_durations()):
                            self.log.debug("Release validated. Recording results.")
                            return result
                self.log.debug("All results failed validation.")
        return False
    
    def run(self, catalog):
        completed = self.data.all_asset_ids_by_catalog("releases")
        _ids = self.replicas[catalog].all_asset_ids()
        _failed = self.data.all_failed_searches_by_catalog("releases")
        assets_to_search = [_id for _id in _ids if all([
            _id not in completed,
            _id not in _failed])]
        for _id in assets_to_search:
            self.log.debug(f"BEGIN - Search {self.api_name} for asset ID {_id}.")
            if self.process_asset(catalog, _id):
                self.log.debug("END - Success")
            else:
                self.log.warning(f"END - Failure")


from interfaces.api.Discogs import Client as DiscogsClient
from interfaces.api.Discogs import Data as DiscogsData

class Discogs(ApiMetadataApp):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path,
                         api_name="Discogs"
                         )
        self.cl = DiscogsClient(sonicat_path)
        self.data = DiscogsData(f"{sonicat_path}/data/metadata/DiscogsMetadata.sqlite")
        

from interfaces.api.Lastfm import Client as LastfmClient
from interfaces.api.Lastfm import Data as LastfmData

class Lastfm(ApiMetadataApp):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path,
                         api_name="Lastfm"
                         )
        self.cl = LastfmClient(sonicat_path)
        self.data = LastfmData(f"{sonicat_path}/data/metadata/LastfmMetadata.sqlite")