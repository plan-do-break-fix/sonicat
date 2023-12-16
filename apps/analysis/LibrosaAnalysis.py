
from decimal import *
import librosa
import numpy as np
import shutil
from typing import Any, Dict, List, Tuple

#from apps.ConfiguredApp import App, Config
from apps.App import AppConfig, SimpleApp
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
        config = AppConfig(sonicat_path, "librosa")
        super().__init__(config)

    def run_cycle(self, task={}):
        if not task:
            self.idle(0)
        elif task["action"] == "basic":
            for _f in task["args"]["file_data"]:
                task["results"] += self.encode_result(self.basic_analysis(_f))
            return task

    def encode_result(self, file_data: dict, result: Dict) -> Dict:
        return [{
                 "file_id": file_data["id"],
                 "data_type": _k,
                 "data": result[_k]
                 }
                 for _k in result.keys()]

    def basic_analysis(self, file_data: Dict) -> List[Dict]:
        path = f"{self.cfg.temp_path}/{file_data['dirname']/file_data['basename']}"
        y, sr, y_harmonic, y_percussive = self.load_wav(path)
        duration = librosa.get_duration(y=y, sr=sr)
        tempo, beat_frames = librosa.beat.beat_track(y=y_percussive, sr=sr)
        duration, tempo = round(duration, 3), round(tempo, 1)
        chromagram = librosa.feature.chroma_cqt(y=y_harmonic, sr=sr)
        cdist = self.chromagram_to_cdist(self.threshold_chromagram(chromagram))
        return {"duration": duration,
                "tempo": tempo,
                "beat_frames": beat_frames,
                "cdist": cdist}

    def load_wav(self, path: str) -> Tuple:
        y, sr = librosa.load(path)
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        return (y, sr, y_harmonic, y_percussive)

    def threshold_chromagram(self, chromagram: np.ndarray) -> np.ndarray:
        for _i, _ in enumerate(chromagram):
            for _j, _ in enumerate(chromagram[_i]):
                if chromagram[_i][_j] < 1.0:
                    chromagram[_i][_j] = 0
        return chromagram
         
    def chromagram_to_cdist(self, chromagram: np.ndarray) -> np.ndarray:
        channel_sums = [sum(_i) for _i in chromagram]
        total = sum(channel_sums)
        return [str(Decimal(_sum/total))[:11] for _sum in channel_sums]
    