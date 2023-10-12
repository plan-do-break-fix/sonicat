
import librosa
import numpy as np
import shutil
from typing import List, Tuple

from apps.ConfiguredApp import App
from interfaces.AudioAnalysis import AnalysisInterface
from interfaces.Catalog import CatalogInterface
from util import Logs
from util.NameUtility import NameUtility


class Analysis(App):
    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        catalog_db_path = f"{self.cfg.data}/catalog/{self.cfg.name}.sqlite"
        self.catalog = CatalogInterface(catalog_db_path)
        self.cfg.log += "/analysis"
        self.cfg.name = "AudioAnalysis"
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"BEGIN {self.cfg.name} application initialization")
        self.dirname = f"{self.cfg.data}/analysis/features/{self.cfg.app_key}"
        self.data = AnalysisInterface(f"{self.cfg.data}/analysis/AudioAnalysis.sqlite")
        self.completed = {"librosa": {}}
        self.load_completed_files()
        self.wav_id = self.catalog.filetype_id("wav")
        self.log.info(f"END application initialization: Success")

    def load_completed_files(self) -> None:
        duration = self.data.all_files_having_data_by_source(self.cfg.app_key, "1", "1")
        tempo = self.data.all_files_having_data_by_source(self.cfg.app_key, "2", "1")
        if not duration == tempo:
            raise RuntimeError
        self.completed["librosa"]["data"] = duration
        self.completed["librosa"]["chroma_dist"] = \
            self.data.all_files_having_feature(self.cfg.app_key, "1")
        self.completed["librosa"]["beat_frames"] = \
            self.data.all_files_having_feature(self.cfg.app_key, "2")

    def librosa_load(self, fpath: str) -> Tuple:
        y, sr = librosa.load(fpath)
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        return (y, sr, y_harmonic, y_percussive)
    
    def filter_completed_wav_ids(self, wav_ids: List[str]) -> List[str]:
        return [_id for _id in wav_ids if not all([
                    _id in self.completed["librosa"]["data"],
                    _id in self.completed["librosa"]["beat_frames"],
                    _id in self.completed["librosa"]["chroma_dist"],
                    ])
                ]

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



class SomethingElse:


    def harmonic_d(cd1, cd2):  # -> Harmonic Distance
        pass

