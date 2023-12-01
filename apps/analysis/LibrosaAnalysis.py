
from decimal import *
import librosa
import numpy as np
import shutil
from typing import List, Tuple

#from apps.ConfiguredApp import App, Config
from apps.ConfiguredApp import SimpleApp
from interfaces.database.LibrosaData import DataInterface
from util.NameUtility import NameUtility
from util.FileUtility import FileUtility


DATATYPES = [
    "tempo",
    "duration"
    "chroma_distribution",
    "beat_frames"
    ]

class LibrosaAnalysis(SimpleApp):

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, "analysis", "LibrosaAnalysis")
        self.basedir = f"{self.cfg.data}/analysis/"
        self.data = DataInterface(f"{self.basedir}/LibrosaAnalysis.sqlite")
        self.file_type_id_cache = {}
        self.load_catalog_replicas()
        self.completed_assets = self.get_completed_assets() 
        self.log.info(f"LibrosaAnalysis Application Initialization Successful")

    def get_completed_assets(self):
        return {_cn: self.data.completed_assets(_cn)
                for _cn in self.config["catalogs"].keys()}

    def get_file_type_id(self, catalog, ext) -> str:
        if catalog not in self.file_type_id_cache.keys():
            self.file_type_id_cache[catalog] = {}
        if ext not in self.file_type_id_cache[catalog].keys():
            _id = self.replicas[catalog].filetype_id(ext)
            self.file_type_id_cache[catalog][ext] = _id
        return self.file_type_id_cache[catalog][ext]

    def load_wav(self, fpath: str) -> Tuple:
        y, sr = librosa.load(fpath)
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        return (y, sr, y_harmonic, y_percussive)
    
    def analyze_wav(self, path) -> Tuple:
        y, sr, y_harmonic, y_percussive = self.load_wav(path)
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
        self.log.debug(f"Checking {catalog} asset ID {asset_id} for wav files.")
        if asset_id in self.completed_assets:
            self.log.debug(f"All wav files for asset ID {asset_id} previously analyzed.")
            return True
        asset_wav_file_ids = self.replicas[catalog].file_ids_by_asset_and_type(
                               asset_id, self.get_file_type_id(catalog, "wav")
                             )
        self.log.debug(f"{len(asset_wav_file_ids)} wav file(s) to analyze in asset.")
        managed_path = self.cfg["catalogs"][catalog]["path"]["managed"]
        cname = self.replicas[catalog].asset_cname(asset_id)
        FileUtility.export_asset_to_temp(cname, managed_path, self.temp)
        label_dir = NameUtility.label_dir_from_cname(cname)
        dfile_base_dir = f"{self.basedir}features/{catalog}/{label_dir}/{cname}"
        if not shutil.os.path.isdir(dfile_base_dir):
            shutil.os.makedirs(dfile_base_dir, exist_ok=True)
        for _id in asset_wav_file_ids:
            self.log.debug(f"Analyzing file ID {_id}.")
            wav_path = f"{self.temp}/{cname}{self.replicas[catalog].file_path(_id)}"
            duration, tempo, beat_frames, chromagram = self.analyze_wav(wav_path)
            cdist = self.chromagram_to_cdist(chromagram)
            self.log.debug("Wav file analysis complete. Writing data to disk.")
            self.write_librosa_data(catalog, _id, duration, tempo)
            self.write_beat_frames_dfile(catalog, _id, beat_frames, dfile_base_dir)
            self.write_chroma_distribution(catalog, _id, cdist)
        self.data.log_completed_asset(catalog, asset_id)
        self.completed_assets[catalog].append(asset_id)
        shutil.rmtree(f"{self.temp}/{cname}")
        self.log.debug(f"Analysis of asset ID {asset_id} wav files complete.")


    def chromagram_to_cdist(self, chromagram: np.ndarray) -> np.ndarray:
        channel_sums = [sum(_i) for _i in chromagram]
        total = sum(channel_sums)
        return [str(Decimal(_sum/total))[:11] for _sum in channel_sums]

    def write_librosa_data(self, catalog, file_id, duration, tempo) -> bool:
        self.data.new_data(file_id, catalog, "1", dvalue=duration, finalize=False)
        self.data.new_data(file_id, catalog, "2", dvalue=tempo, finalize=False)
        self.data.db.commit()
        return True
    
    def write_beat_frames_dfile(self, catalog, file_id, beat_frames, dfile_base_dir):
        full_datapath = f"{dfile_base_dir}/{file_id}-librosa-beat_frames.npy"
        np.save(full_datapath, beat_frames)
        datapath = full_datapath.replace(self.cfg.data, "data")
        self.data.new_data(file_id, catalog, "4", dpath=datapath, finalize=False)
        return True

    def write_chroma_distribution(self, catalog, file_id, chroma_distribution) -> str:
        self.data.new_chroma_distribution(catalog, file_id, chroma_distribution)
        self.data.c.execute("SELECT last_insert_rowid();")
        cdist_id = self.data.c.fetchone()[0]
        self.data.new_data(file_id, catalog, "3", dkeyid=cdist_id, finalize=False)
        return True

    def run(self, catalog):
        all_ids = self.replicas[catalog].all_asset_ids()
        assets_to_process = [_id for _id in all_ids if _id not in self.completed_assets[catalog]]
        for _id in assets_to_process:
            self.analyze_asset_wavs(catalog, _id)
        return True
    
