

from apps.ConfiguredApp import SimpleApp
from interfaces.database.LibrosaData import DataInterface
import re

from typing import Dict, List, Tuple



class Data(SimpleApp):

    def __init__(self, sonicat_path) -> None:
        super().__init__(sonicat_path, "analysis", "DataOperations")
        if sonicat_path == "":
            return None
        self.data = DataInterface(f"{sonicat_path}/data/analysis/LibrosaAnalysis-ReadReplica.sqlite")
        self.load_catalog_replicas()

    def asset_audio_file_data(self, catalog, asset_id, filetype="wav") -> List[Dict]:
        filetype_id = self.replicas[catalog].filetype_id(filetype)
        return self.replicas[catalog].file_data_by_asset_and_type(asset_id, filetype_id)

    def multidisc_folders(self, catalog, asset_id, filetype="wav") -> bool:
        """Returns True if all audio file dirnames start with "/CD"."""
        file_data = self.asset_audio_file_data(catalog, asset_id)
        return all(["/CD" in _d["dirname"] and len(_d["dirname"]) > 3 for _d in file_data])

    def disc_number(self, dirname: str) -> str:
        """Returns the integer string of the file's CD subdirectory number."""
        if not dirname.startswith("/CD"):
            return ""
        raw_number = dirname[3:]
        try:
            number = int(raw_number)
        except ValueError:
            print(f"Expect disc number, got: {raw_number}")
            return ""
        return str(number)
     
    def common_starting_substring(self, strings: List[str]) -> str:
        if len(strings) == 1:
            return strings[0]
        _abort_index = min([len(_s) for _s in strings])
        common = ""
        for _i in range(1, _abort_index):
            if len(set([_s[:_i] for _s in strings])) == 1:
                common = strings[0][:_i]
        return common

    def parse_common_substring(self, substring: str) -> str:
        """
        Returns the portion of a common starting substring that should be dropped.
        """
        to_drop = ""
        for _i in range(1, len(substring)):
            if re.search(r"[A-J0-9]", substring[_i]):
                to_drop += substring[_i]
            else:
                return to_drop

    def prepare_filenames(self, track_list: List[Tuple[str, float]]):
        titles = [_t[0] for _t in track_list]
        _drop = self.parse_common_substring(self.common_starting_substring(titles))
        for _i, _t in enumerate(track_list):
            track_list[_i][0] = track_list[_i][0][len(_drop):]

    def order_track_list(self, tracklist) -> List[Tuple[str]]:
        indexed_tracks = []
        if re.search(r"^\d{1,2}[_ \-\.]*\d{1,3}", tracklist[0][0]):
            for _t in tracklist:
                disc = int(re.search(r"^\d{1,2}", _t[0].strip()).group())
                index_res = re.search(r"(?<=\d[_ \-\.])\d{1,3}", _t[0].strip())
                index = int(index_res.group()) if index_res else ""
                indexed_tracks.append((disc, index, _t))
        elif re.search(r"^[a-zA-Z][_\-\. ]*\d{1,3}", tracklist[0][0]):
            for _t in tracklist:
                disc = _t[0]
                index_res = re.search(r"(?<=[_\-\. ])\d{1,3}", _t[0].strip())
                index = int(index_res.group()) if index_res else ""
                indexed_tracks.append((disc, index, _t))
        elif re.search(r"^\d{1,3}", tracklist[0][0]):
            for _t in tracklist:
                index = int(re.search(r"^\d{1,3}", _t[0].strip()).group())
                indexed_tracks.append((index, _t))
        else:
            return False
        indexed_tracks.sort()
        return [_i[-1] for _i in indexed_tracks]

    def asset_track_list(self, catalog, asset_id, filetype="wav") -> List[Tuple[str, float]]:
        """
        For each track: (title, duration, catalog, file_id)
        """
        file_data = self.asset_audio_file_data(catalog, asset_id)
        file_ids = [_d["id"] for _d in file_data]
        if not file_ids:
            return []
        _dtype, dtype_id = "duration", "1"
        durations = self.data.data_values(file_ids, catalog, dtype_id)
        duration_dict = {_d[0]: _d[1] for _d in durations}
        if not len(file_ids) == len(durations):
            return []
        if self.multidisc_folders(catalog, asset_id):
            name_duration_pairs = []
            for _d in file_data:
                disc = self.disc_number(_d["dirname"])
                name_duration_pairs.append((f"{disc}-{_d['basename']}", duration_dict[_d["id"]], catalog, _d["id"]))
        else:
            name_duration_pairs = [(_d["basename"], duration_dict[_d["id"]], catalog, _d["id"]) for _d in file_data]
        return self.order_track_list(name_duration_pairs)

