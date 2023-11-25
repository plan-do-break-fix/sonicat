

from apps.ConfiguredApp import App
from interfaces.database.Catalog import CatalogInterface
from interfaces.database.LibrosaData import DataInterface
import re

from typing import Dict, List, Tuple
from decimal import Decimal



class Data(App):

    def __init__(self, sonicat_path) -> None:
        super().__init__(sonicat_path, "")
        self.catalog = {
            #"assets": CatalogInterface(f"{self.cfg.data}/catalog/AssetCatalog.sqlite"),
            "releases": CatalogInterface(f"{self.cfg.data}/catalog/ReleaseCatalog-ReadReplica.sqlite")
        } 
        self.data = DataInterface(f"{self.cfg.data}/analysis/LibrosaAnalysis-ReadReplica.sqlite")

    def asset_audio_file_data(self, catalog, asset_id, filetype="wav") -> List[Dict]:
        filetype_id = self.catalog[catalog].filetype_id(filetype)
        return self.catalog[catalog].all_file_data_by_asset(asset_id, filetype_id)


    def multidisc_folders(self, catalog, asset_id, filetype="wav") -> bool:
        file_data = self.asset_audio_file_data(catalog, asset_id)
        return all(["/CD" in _d["dirname"] for _d in file_data])
    
    def disc_number(self, dirname: str) -> str:
        if not dirname.startswith("/CD"):
            raise ValueError
        raw_number = dirname[3:]
        try:
            number = int(raw_number)
        except ValueError:
            print(f"Expect disc number, got: {raw_number}")
        return str(number)

    def order_track_list(self, tracklist) -> List[Tuple[str]]:
        indexed_tracks = []
        if re.search(r"^\d{1,2}[_ \-\.]\d{1,3}", tracklist[0][0]):
            for _t in tracklist:
                disc = int(re.search(r"^\d{1,2}", _t[0].strip()).group())
                index = int(re.search(r"(?<=\d[_ \-\.])\d{1,3}", _t[0].strip()).group())
                indexed_tracks.append((disc, index, _t[0], _t[1]))
        else:
            for _t in tracklist:
                index = int(re.search(r"^\d{1,3}", _t[0].strip()).group())
                indexed_tracks.append((index, _t[0], _t[1]))
        indexed_tracks.sort()
        return [(_i[-2], _i[-1]) for _i in indexed_tracks]

    def asset_track_list(self, catalog, asset_id, filetype="wav") -> List[Tuple[str, Decimal]]:
        file_data = self.asset_audio_file_data(catalog, asset_id)
        file_ids = [_d["id"] for _d in file_data]
        _dtype, dtype_id = "duration", "1"
        durations = self.data.data_values(file_ids, catalog, dtype_id)
        duration_dict = {_d[0]: _d[1] for _d in durations}
        if not len(file_ids) == len(durations):
            raise RuntimeError
        if self.multidisc_folders(catalog, asset_id):
            name_duration_pairs = []
            for _d in file_data:
                disc = self.disc_number(_d["dirname"])
                name_duration_pairs.append((f"{disc}-{_d['basename']}", duration_dict[_d["id"]]))
        else:
            name_duration_pairs = [(_d["basename"], duration_dict[_d["id"]]) for _d in file_data]
        return self.order_track_list(name_duration_pairs)

