
from decimal import Decimal
#import functools
import librosa
import numpy as np
import shutil
from typing import List, Tuple

from apps.ConfiguredApp import App
from interfaces.LibrosaData import DataInterface
from interfaces.Catalog import CatalogInterface
from interfaces.Interface import DatabaseInterface
from util import Logs
from util.NameUtility import NameUtility


DATATYPES = [
        "tempo",
        "duration"
        "chroma_distribution",
        "beat_frames"
]

class LibrosaAnalysis(App):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, "")
        self.basedir = f"{self.cfg.data}/analysis/"
        self.data = DataInterface(f"{self.basedir}/AudioAnalysis.sqlite")
        self.catalog = {}
        for _catalog in self.catalog_names:
            catalog_db_path = f"{self.cfg.data}/catalog/{_catalog}.sqlite"
            self.catalog[_catalog] = CatalogInterface(catalog_db_path)
        self.completed_assets = {_catalog: self.data.completed_assets(_catalog)
                            for _catalog in self.catalog_names}

    def get_file_type_id(self, catalog, ext) -> str:
        if catalog not in self.file_type_id_cache.keys():
            self.file_type_id_cache[catalog] = {}
        if ext not in self.file_type_id_cache[catalog].keys():
            _id = self.catalog[catalog].filetype_id(ext)
            self.file_type_id_cache[catalog][ext] = _id
        return self.file_type_id_cache[catalog][ext]

    def load_wav(self, fpath: str) -> Tuple:
        y, sr = librosa.load(fpath)
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        return (y, sr, y_harmonic, y_percussive)

    def analyze_wav(self, path) -> Tuple:
        y, sr, y_harmonic, y_percussive = self.librosa_load(path)
        duration = librosa.get_duration(y=y, sr=sr)
        tempo, beat_frames = librosa.beat.beat_track(y=y_percussive, sr=sr)
        duration, tempo = round(duration, 3), round(tempo, 1)
        chromagram = librosa.feature.chroma_cqt(y=y_harmonic, sr=sr)
        for _i, _ in enumerate(chromagram):
            for _j, _ in enumerate(chromagram[_i]):
                if chromagram[_i][_j] < 1.0:
                    chromagram[_i][_j] = 0
        return (duration, tempo, beat_frames, chromagram)

    def analyze_asset_wavs(self, catalog, asset_id) -> bool:
        self.log.debug(f"Checking asset {catalog}.{asset_id} wav files.")
        if asset_id in self.completed_assets:
            self.log.debug(f"All wav files for asset ID {asset_id} previously analyzed.")
            return True
        asset_wav_file_ids = self.catalog[catalog].file_ids_by_asset_and_type(
                               asset_id, self.get_file_type_id("wav")
                             )
        self.log.debug(f"{len(asset_wav_file_ids)} wav files found.")
        cname = self.catalog.asset_cname(asset_id)
        label_dir = NameUtility.label_dir_from_cname(cname)
        for _id in asset_wav_file_ids:
            wav_path = f"{self.cfg.temp}/{cname}{self.catalog.file_path(_id)}"
            duration, tempo, beat_frames, chromagram = self.analyze_wav(wav_path)
            cdist = self.chromagram_to_cdist(chromagram)
            self.write_librosa_data(catalog, _id, duration, tempo)
            self.write_beat_frames_dfile(catalog, _id, beat_frames, label_dir, cname)
            self.write_chroma_distribution(catalog, _id, cdist)
        self.data.log_completed_asset(catalog, asset_id)
        self.completed_assets[catalog].append(asset_id)

    def chromagram_to_cdist(self, chromagram: np.ndarray) -> np.ndarray:
        channel_sums = [sum(_i) for _i in chromagram]
        total = sum(channel_sums)
        return [Decimal(_sum/total) for _sum in channel_sums]

    def write_librosa_data(self, catalog, file_id, duration, tempo) -> bool:
        self.data.new_data(file_id, catalog, "1", dvalue=duration, finalize=False)
        self.data.new_data(file_id, catalog, "2", dvalue=tempo, finalize=False)
        self.data.db.commit()
        return True
    
    def write_beat_frames_dfile(self, catalog, file_id, beat_frames, label_dir, cname):
        dirname = f"{self.basedir}/features/{catalog}/{label_dir}/{cname}"
        full_datapath = f"{dirname}/{file_id}-librosa-beat_frames.npy"
        np.save(full_datapath, beat_frames)
        datapath = full_datapath.replace(self.cfg.data, "data")
        self.data.new_data(file_id, catalog, "4", dpath=datapath, finalize=False)
        return True

    def write_chroma_distribution(self, catalog, file_id, chroma_distribution) -> str:
        self.data.new_chroma_distribution(catalog, file_id, chroma_distribution)
        self.c.execute("SELECT last_insert_rowid();")
        cdist_id = self.c.fetchone()[0]
        self.data.new_data(file_id, catalog, "3", dkeyid=cdist_id, finalize=False)
        return True

'''
LIBRARIES = ["librosa"]
DATA = {
    "librosa": [
    ]
}
FEATURES = {
    "librosa": [
        "beat_frames",
        "chromagram_hard_threshold"
    ]
}

class AnalysisApp(App):
    """
    Base class for all univariate analysis, to be extended for each implemented 
    analytical library.
    See: AudioAnalysisApp.LIBRARIES
    """
    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, "")
        self.basedir = f"{self.cfg.data}/analysis/"
        self.data = AnalysisInterface(f"{self.basedir}/AudioAnalysis.sqlite")
        self.catalog = {}
        for _catalog in self.catalog_names:
            catalog_db_path = f"{self.cfg.data}/catalog/{_catalog}.sqlite"
            self.catalog[_catalog] = CatalogInterface(catalog_db_path)
        self.completed = {_l: {_c: {"data":{}, "features":{}}
                               for _c in self.catalog_names}
                          for _l in LIBRARIES}
        self.initialize_completed_files()

        self.file_type_id_cache = {}

    def get_file_type_id(self, catalog, ext) -> str:
        if catalog not in self.file_type_id_cache.keys():
            self.file_type_id_cache[catalog] = {}
        if ext not in self.file_type_id_cache[catalog].keys():
            _id = self.catalog[catalog].filetype_id(ext)
            self.file_type_id_cache[catalog][ext] = _id
        return self.file_type_id_cache[catalog][ext]

    def initialize_completed_files(self) -> bool:
        """
        Populates completed file IDs-by-job list for all libraries, catalogs,
        job types, and jobs.
        """
        for library in LIBRARIES:
            for catalog in self.catalog_names:
                for _d in DATA[library]:
                    _d_id = self.data.dtype_id(_d)
                    self.completed[library][catalog]["data"][_d] = \
                            self.data.all_files_having_data(catalog, _d_id)
                for _f in FEATURES[library]:
                    _f_id = self.data.ftype_id(_f)
                    self.completed[library][catalog]["features"][_f] = \
                            self.data.all_files_having_feature(catalog, _f_id)
        return True

    def all_data_jobs_complete(self, library, catalog, file_id: str) -> bool:
        return all([file_id in self.completed[library][catalog]["data"][_job]
                    for _job in DATA[library].keys()])
    
    def all_feature_jobs_complete(self, library, catalog, file_id: str) -> bool:
        return all([file_id in self.completed[library][catalog]["features"][_job]
                    for _job in FEATURES[library].keys()])
    
    def filter_completed_wav_ids(self, library: str,
                                       catalog: str,
                                       wav_ids: List[str]
                                       ) -> List[str]:
        return [_id for _id in wav_ids
                if not(self.all_data_jobs_complete(library, catalog, _id) and 
                       self.all_feature_jobs_complete(library, catalog, _id))
                ]


class Librosa(APP):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, "")
        self.basedir = f"{self.cfg.data}/analysis/"
        self.data = AnalysisInterface(f"{self.basedir}/AudioAnalysis.sqlite")
        self.catalog = {}
        #for _catalog in self.catalog_names:
        #    catalog_db_path = f"{self.cfg.data}/catalog/{_catalog}.sqlite"
        #    self.catalog[_catalog] = CatalogInterface(catalog_db_path)
        self.completed = {_l: {_c: {"data":{}, "features":{}}
                               for _c in self.catalog_names}
                          for _l in LIBRARIES}
        #self.initialize_completed_files()
        self.file_type_id_cache = {}
        self.cfg.log += "/analysis"
        self.cfg.name = "AudioAnalysis"
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"Application Initialization Successful")

    def load_wav(self, fpath: str) -> Tuple:
        y, sr = librosa.load(fpath)
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        return (y, sr, y_harmonic, y_percussive)
    
    def analyze_asset_wavs(self, catalog, asset_id) -> bool:
        self.log.debug(f"Checking asset {catalog}.{asset_id} wav files.")
        asset_wav_file_ids = self.catalog[catalog].file_ids_by_asset_and_type(
                               asset_id, self.get_file_type_id("wav")
                             )
        self.log.debug(f"{len(asset_wav_file_ids)} wav files found.")
        asset_wav_file_ids = self.filter_completed_wav_ids(
                             "librosa", catalog, asset_wav_file_ids
                             )
        if not asset_wav_file_ids:
            self.log.debug("Analysis complete for all found wav files.")
            return True
        cname = self.catalog.asset_cname(asset_id)
        label_dir = NameUtility.label_dir_from_cname(cname)

        for _id in asset_wav_file_ids:
            wav_path = f"{self.cfg.temp}/{cname}{self.catalog.file_path(_id)}"
            duration, tempo, beat_frames, chromagram = self.analyze_wav(wav_path)
            self.write_librosa_data(catalog, _id, duration, tempo)
            self.log.debug(f"Librosa data values recorded for file ID {_id}")
            self.write_librosa_features(catalog, _id, beat_frames, chromagram, label_dir, cname)
            self.log.debug(f"Librosa features recorded for file ID {_id}")

    def analyze_wav(self, path) -> Tuple:
        y, sr, y_harmonic, y_percussive = self.librosa_load(path)
        duration = librosa.get_duration(y=y, sr=sr)
        tempo, beat_frames = librosa.beat.beat_track(y=y_percussive, sr=sr)
        duration, tempo = round(duration, 3), round(tempo, 1)
        chromagram = librosa.feature.chroma_cqt(y=y_harmonic, sr=sr)
        for _i, _ in enumerate(chromagram):
            for _j, _ in enumerate(chromagram[_i]):
                if chromagram[_i][_j] < 1.0:
                    chromagram[_i][_j] = 0
        return (duration, tempo, beat_frames, chromagram)

    def write_librosa_data(self, catalog, file_id, duration, tempo) -> bool:
        self.data.new_data(file_id, catalog, "1", duration, "3", finalize=False)
        self.data.new_data(file_id, catalog, "2", tempo, "3", finalize=False)
        self.data.db.commit()
        self.completed["librosa"][catalog]["data"]["duration"].append(file_id)
        self.completed["librosa"][catalog]["data"]["tempo"].append(file_id)
        return True


    def write_librosa_features(self, catalog: str,
                                     file_id: str,
                                     beat_frames: np.ndarray,
                                     chromagram: np.ndarray,
                                     label_dir: str,
                                     cname: str
                                     ) -> bool:
        dirname = f"{self.basedir}/features/{catalog}/{label_dir}/{cname}"

        full_datapath = f"{dirname}/{file_id}-librosa-beat_frames.npy"
        np.save(full_datapath, beat_frames)
        datapath = full_datapath.replace(self.cfg.data, "data")
        self.data.new_feature(file_id, self.cfg.app_key, "2", datapath, "3", finalize=True)
        self.completed["librosa"][catalog]["features"]["beat_frames"].append(file_id)
        
        full_datapath = f"{dirname}/{file_id}-librosa-chromagram.npy"
        np.save(full_datapath, chromagram)
        datapath = full_datapath.replace(self.cfg.data, "data")
        self.data.new_feature(file_id, self.cfg.app_key, "1", datapath, "3", finalize=True)
        self.completed["librosa"][catalog]["features"]["chromagram"].append(file_id)
'''


# To prevent duplication of commutative operations, given the 4-tuple:
#   (catalog1, asset_id1, catalog2, asset_id2)
# it is expected catalog1 <= catalog2.
# If catalog1 = catalog2, it is expected asset_id1 < asset_id2.
HARMONIC_DISTANCE_SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS data (
  id integer PRIMARY KEY,
  catalog1 text NOT NULL,
  file1 integer NOT NULL,
  catalog2 text NOT NULL,
  file2 integer NOT NULL,
  distance float NOT NULL
);
"""
]

class HarmonicDistanceData(DatabaseInterface):

    def __init__(self, dbpath=""):
        for statement in HARMONIC_DISTANCE_SCHEMA:
            self.c.execute(statement)
        super().__init__(dbpath)

    def initialize_completed_files(self) -> bool:
        for library in LIBRARIES:
            for catalog in self.catalog_names:
                for _d in DATA[library]:
                    _d_id = self.data.dtype_id(_d)
                    self.completed[library][catalog]["data"][_d] = \
                            self.data.all_files_having_data(catalog, _d_id)
                for _f in FEATURES[library]:
                    _f_id = self.data.ftype_id(_f)
                    self.completed[library][catalog]["features"][_f] = \
                            self.data.all_files_having_feature(catalog, _f_id)
        return True

    def add_distance(self, catalog1, id1, catalog2, id2, distance) -> bool:
        self.c.execute("INSERT INTO data"\
                       "  (catalog1, file1, catalog2, file2, distance)"\
                       " VALUES (?,?,?,?,?);",
                       (catalog1, id1, catalog2, id2, distance))
        self.db.commit()
        return True
    
    def all_data(self):
        self.c.execute("SELECT * FROM data;")
        return self.c.fetchall()

    def all_data_by_catalog(self, catalog1, catalog2=""):
        catalog2 = catalog1 if not catalog2 else catalog2
        self.c.execute("SELECT * FROM data WHERE catalog1 = ? AND catalog2 = ?;",
                       (catalog1, catalog2))
        return self.c.fetchall()

    def distance(self, catalog1, id1, catalog2, id2):
        self.c.execute("SELECT distance FROM data"\
                       " WHERE catalog1 = ? AND file1 =?"\
                       " AND catalog2 = ? AND file2 = ?",
                       (catalog1, id1, catalog2, id2))
        result = self.c.fetchone()[0]
        return Decimal(result)
    
    def all_data_by_file(self, catalog, file_id) -> List[Decimal]:
        self.c.execute("SELECT * FROM data"\
                       "  WHERE (catalog1 = ? AND file1 = ?)"\
                       "  OR (catalog2 = ? AND file2 = ?);",
                       (catalog, file_id))
        return self.c.fetchall()


class HarmonicDistance(AnalysisApp):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, "")

        results_db_path = f"{self.cfg.data}/analysis/HarmonicDistance.sqlite"
        self.results = HarmonicDistanceData(results_db_path)
        self.cds = {_catalog: {} for _catalog in self.catalog_names} # chromatic distributions
        self.cfg.log += "/analysis"
        self.cfg.name = "HarmonicDistance"
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"Application Initialization Successful")

    #def load_chromagram(self, catalog: str, file_id: str) -> np.ndarray:
    #    datapath = self.data.feature_data_path(file_id, catalog, ftype_id=1)
    #    return np.load(datapath)
    

    
    def get_completed_intracatalog_pairs(self, catalog: str) -> List[str]:
        return [(_i[1], _i[3]) for _i in self.results.all_data_by_catalog(catalog)]

    def get_intraasset_pairs(self, catalog):
        wav_id = self.catalog[catalog].filetype_id("wav")
        file_asset_pairs = self.catalog[catalog].file_asset_id_pairs(filetype_ids=[wav_id])
        output = []
        for _p1 in file_asset_pairs:
            for _p2 in file_asset_pairs:
                if _p1 == _p2:
                    continue
                if _p1[1] == _p2[1]:
                    output.append((_p1[0], _p2[0]))
        return output

    def get_intralabel_pairs(self, catalog):
        wav_id = self.catalog[catalog].filetype_id("wav")
        label_file_pairs = self.catalog[catalog].label_file_id_pairs(filetype_ids=[wav_id])
        output = []
        for _p1 in label_file_pairs:
            for _p2 in label_file_pairs:
                if _p1 == _p2:
                    continue
                if _p1[0] == _p2[0]:
                    output.append((_p1[1], _p2[1]))
        return output

    def intracatalog_pairs_to_calculate(self, catalog) -> List[Tuple[str]]:
        """
        Returns list of file ID pairs for files that:
         - do not have the same label
         - have a calculated chromagram on file
         - do not have a harmonic distance on file
        """
        completed_pairs = self.get_completed_intracatalog_pairs(catalog)
        intralabel_pairs = self.get_intralabel_pairs(catalog)
        self.completed[catalog]["features"]["chromagram_hard_threshold"].sort()
        to_calculate = []
        for _id1 in self.completed[catalog]["features"]["chromagram_hard_threshold"]:
            for _id2 in self.completed[catalog]["features"]["chromagram_hard_threshold"]:
                if _id1 == _id2:
                    continue
                _pair = (_id1, _id2) if _id1 < _id2 else (_id2, _id1)
                if _pair in completed_pairs:
                    continue
                if _pair in intralabel_pairs:
                    continue
                to_calculate.append(_pair)
        return to_calculate

    def chroma_distribution(self, catalog, file_id) -> np.ndarray:
        if not file_id in self.cds[catalog].keys():
            self.cds[catalog][file_id] = \
                    self.chromagram_to_cd(self.load_chromagram(catalog, file_id))
        return self.cds[catalog][file_id]

    def linear_intracatalog_run(self, catalog):
        self.log.debug(f"Initializing data for linear intracatalog run: {catalog}")
        pairs_to_calculate = self.intracatalog_pairs_to_calculate(catalog)
        self.log.debug(f"{len(pairs_to_calculate)} pair combinations to calculate")
        for _p in pairs_to_calculate:
            cd1 = self.chroma_distribution(catalog, _p[0])
            cd2 = self.chroma_distribution(catalog, _p[1])
            distance = self.harmonic_distance(cd1, cd2)
            if not distance:
                raise RuntimeError
            self.results.add_distance(catalog, _p[0], catalog, _p[1], distance)
            self.log(f"Harmonic distance recorded for intracatalog pair {_p[0]}, {_p[1]}")

    def result_fname(self, catalog1, asset_id1, catalog2, asset_id2) -> str:
        return f"{catalog1}_{asset_id1}-{catalog2}_{asset_id2}.csv"

    def harmonic_distance(self, chroma_dist1, chroma_dist2) -> Decimal:
        return sum([(chroma_dist1[_i] - chroma_dist2[_i])**2 
                    for _i in range(0, len(chroma_dist1))]
                   )

    #might be needed later, but not for linear intracatalog processing 
    def reorder_id_pairs(self, catalog1, id1, catalog2, id2):
            if catalog1 == catalog2:
                ids = [id1, id2]
                ids.sort()
                if not ids[0] == id1:
                    return (catalog1, id2, catalog2, id1)
            catalogs = [catalog1, catalog2]
            catalogs.sort()
            if not catalogs[0] == catalog1:
                return (catalog2, id2, catalog1, id1)
            return (catalog1, id1, catalog2, id2)

        #def load_cds_by_assets(self, assets: List[str]):
    #    all_ids_by_catalog = {}
    #    for asset in assets:
    #        if asset[0] not in all_ids_by_catalog:
    #            all_ids_by_catalog[asset[0]] = []
    #        all_ids_by_catalog[asset[0]] = self.interfaces[asset[0]].file_ids_by_asset(asset[1])
    #    for catalog in all_ids_by_catalog.keys():
    #        self.cds[catalog][asset[1]] = self.chroma_dist(self.l)


    #    def file_id_pairs(self, catalog1, asset_id1, catalog2, asset_id2) -> List[List]:
#        catalog1, asset_id1, catalog2, asset_id2 = \
#                      self.order_id_pairs(catalog1, asset_id1, catalog2, asset_id2)
#        file_ids1 = self.__getattribute__(catalog1).file_ids_by_asset(asset_id1)
#        file_ids2 = self.__getattribute__(catalog2).file_ids_by_asset(asset_id2)
#        pairs = []
#        for _f1 in file_ids1:
#            for _f2 in file_ids2:
#                pairs.append(self.order_id_pairs(catalog1, _f1, catalog2, _f2))
#        return pairs




'''
class Analysis(App):
    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        catalog_db_path = f"{self.cfg.data}/catalog/{self.cfg.name}.sqlite"
        self.catalog = CatalogInterface(catalog_db_path)
        self.cfg.log += "/analysis"
        self.cfg.name = "AudioAnalysis"
        self.log = Logs.initialize_logging(self.cfg)
        self.dirname = f"{self.cfg.data}/analysis/features/{self.cfg.app_key}"
        self.data = AnalysisInterface(f"{self.cfg.data}/analysis/AudioAnalysis.sqlite")
        self.completed = {"librosa": {}}
        self.load_completed_files()
        self.wav_id = self.catalog.filetype_id("wav")

    #def load_completed_files(self) -> None:
    #    duration = self.data.all_files_having_data_by_source(self.cfg.app_key, "1", "1")
    #    tempo = self.data.all_files_having_data_by_source(self.cfg.app_key, "2", "1")
    #    if not duration == tempo:
    #        raise RuntimeError
    #    self.completed["librosa"]["data"] = duration
    #    self.completed["librosa"]["chroma_dist"] = \
    #        self.data.all_files_having_feature(self.cfg.app_key, "1")
    #    self.completed["librosa"]["beat_frames"] = \
    #        self.data.all_files_having_feature(self.cfg.app_key, "2")

    #def librosa_load(self, fpath: str) -> Tuple:
    #    y, sr = librosa.load(fpath)
    #    y_harmonic, y_percussive = librosa.effects.hpss(y)
    #    return (y, sr, y_harmonic, y_percussive)
    
    #def filter_completed_wav_ids(self, wav_ids: List[str]) -> List[str]:
    #    return [_id for _id in wav_ids if not all([
    #                _id in self.completed["librosa"]["data"],
    #                _id in self.completed["librosa"]["beat_frames"],
    #                _id in self.completed["librosa"]["chroma_dist"],
    #                ])
    #            ]

    def analyze_asset_audio_file(self, asset_id: str) -> bool:
        cname = self.catalog.asset_cname(asset_id)
        self.log.info(f"BEGIN Asset audio file analysis - Asset ID {asset_id}, {cname}")
        all_asset_wav_ids = self.catalog.file_ids_by_asset_and_type(asset_id, self.wav_id)
        target_wav_ids = self.filter_completed_wav_ids(all_asset_wav_ids)
        if len(target_wav_ids) == 0:
            self.log.debug(f"All wav files in asset ID {asset_id} already analyzed")
            return True
        self.catalog.export_asset_to_temp(asset_id, self.cfg)
        self.log.debug(f"Asset archive restored to {self.cfg.temp}")
        label_dir = NameUtility.label_dir_from_cname(cname)
        dirname_base = f"{self.dirname}/{label_dir}/{cname}"
        if not shutil.os.path.isdir(dirname_base):
            shutil.os.makedirs(dirname_base, exist_ok=True)
        self.log.debug(f"{len(target_wav_ids)} wav file(s) found in asset")
        for _id in target_wav_ids:
            self.log.debug(f"BEGIN processing file ID {_id}")
            if _id not in self.completed["librosa"]["data"]:
                fpath = f"{self.cfg.temp}/{cname}{self.catalog.file_path(_id)}"
                y, sr, y_harmonic, y_percussive = self.librosa_load(fpath)
                duration = librosa.get_duration(y=y, sr=sr)
                tempo, beat_frames = librosa.beat.beat_track(y=y_percussive, sr=sr)
                duration, tempo = round(duration, 3), round(tempo, 1)
                self.data.new_data(_id, self.cfg.app_key, "1", duration, "3", finalize=False)
                self.data.new_data(_id, self.cfg.app_key, "2", tempo, "3", finalize=False)
                self.data.db.commit()
                self.completed["librosa"]["data"].append(_id)
                self.log.debug(f"Librosa data values recorded for file ID {_id}")
            else:
                self.log.debug(f"Librosa data already exists for file ID {_id}")
            if _id not in self.completed["librosa"]["beat_frames"]:
                datapath = f"{dirname_base}/{_id}-librosa-beat_frames.npy"
                np.save(datapath, beat_frames)
                datapath = datapath.replace(self.cfg.data, "data")
                self.data.new_feature(_id, self.cfg.app_key, "2", datapath, "3", finalize=True)
                self.completed["librosa"]["beat_frames"].append(_id)
                self.log.debug(f"Librosa beat frames feature recorded for file ID {_id}")
            else:
                self.log.debug(f"Librosa beat frames feature already exists for file ID {_id}")
            if _id not in self.completed["librosa"]["chroma_dist"]:
                chroma_dist = librosa.feature.chroma_cqt(y=y_harmonic, sr=sr)
                for _i, _ in enumerate(chroma_dist):
                    for _j, _ in enumerate(chroma_dist[_i]):
                        if chroma_dist[_i][_j] < 1.0:
                            chroma_dist[_i][_j] = 0
                datapath = f"{dirname_base}/{_id}-librosa-chroma_dist.npy"
                np.save(datapath, chroma_dist)
                datapath = datapath.replace(self.cfg.data, "data")
                self.data.new_feature(_id, self.cfg.app_key, "1", datapath, "3", finalize=True)
                self.completed["librosa"]["chroma_dist"].append(_id)
                self.log.debug(f"Librosa chroma dist feature recorded for file ID {_id}")
            else:
                self.log.debug(f"Librosa chroma dist feature already exists for file ID {_id}")
            self.log.debug(f"END processing file ID {_id}: Success")
        shutil.rmtree(f"{self.cfg.temp}/{cname}")
        self.log.info(f"END asset ID {asset_id} audio file analysis: Success")
        return True

'''

