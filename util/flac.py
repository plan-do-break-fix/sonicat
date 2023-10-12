
import re, shutil, subprocess
from typing import Dict, List

def cue2dict(path) -> Dict:
    out = {"tracks": {}}
    with open(path, "rb") as _f:
        for _encoding in ["utf-8", "utf-16", "iso-8859-1", "cp1252", "latin-1", "ascii"]:
            try:
                lines = [_l.decode(_encoding).strip() for _l in _f.readlines()]
                break
            except UnicodeDecodeError:
                continue
    if not lines:
        raise RuntimeWarning
    for _i, _l in enumerate(lines):
        if _l.startswith("REM DATE"):
            out["year"] = _l.split(" ")[-1]
        elif _l.startswith("PERFORMER"):
            out["artist"] = _l.split('"')[-2]
        elif _l.startswith("TITLE"):
            out["title"] = _l.split('"')[-2]
        elif _l.startswith("TRACK"):
            ordinal = _l.split(" ")[1]
            title = lines[_i+1].split('"')[-2]
            artist = lines[_i+2].split('"')[-2]
            out["tracks"][ordinal] = {"title": title, "artist": artist}
    return out


def is_compilation(cuedict) -> bool:
    track_artists = [cuedict["tracks"][_i]["artist"]
                     for _i in cuedict["tracks"].keys()
                     ]
    if len(set(track_artists)) == 1:
        return False
    elif len(set(track_artists)) > 1:
        return True


def file_names(cuedict) -> List[str]:
    out, compilation = [], is_compilation(cuedict)
    for _t in cuedict["tracks"].keys():
        _n = f"{_t} - "
        if compilation:
            _n += f"{cuedict['tracks'][_t]['artist']} - "
        _n += f"{cuedict['tracks'][_t]['title']}.wav"
        out.append(_n)
    out.sort()
    return out

def make_tracks(path) -> bool:
    initial_dir = shutil.os.getcwd()
    shutil.os.chdir(path)
    if not prerun_check(path):
        return False
    cue_path = [_i for _i in shutil.os.listdir(path) if _i.endswith(".cue")][0]
    cue_path = shutil.os.path.abspath(f"{path}/{cue_path}")
    _dict = cue2dict(cue_path)
    split_flac(path)
    fnames = file_names(_dict)
    wav_files = [_i for _i in shutil.os.listdir(path) if _i.endswith(".wav")]
    if not len(fnames) == len(wav_files):
        raise RuntimeError
    fnames.sort(), wav_files.sort()
    for _pair in zip(wav_files, fnames):
        shutil.move(_pair[0], _pair[1])
    shutil.os.chdir(initial_dir)
    return True

def prerun_check(path):
    if any([count_by_ext(path, "flac") != 1,
            count_by_ext(path, "wav") > 0
            ]):
        return False
    return True

def count_by_ext(path, ext) -> int:
    return len([_i for _i in shutil.os.listdir(path) if _i.endswith(f".{ext}")])

def split_flac(path):
    flac_path = [_i for _i in shutil.os.listdir(path) if _i.endswith(".flac")][0]
    flac_path = shutil.os.path.abspath(f"{path}/{flac_path}")
    cue_path = [_i for _i in shutil.os.listdir(path) if _i.endswith(".cue")][0]
    cue_path = shutil.os.path.abspath(f"{path}/{cue_path}")
    cmd = f"cuebreakpoints \"{cue_path}\" | shnsplit -o wav \"{flac_path}\""
    ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = ps.communicate()[0]
    return True

def list_flacs(path) -> List[str]:
    return [f"{path}/{_f}"
            for _f in shutil.os.listdir(path)
            if _f.endswith(".flac")
            ]

def flac2wav(path) -> bool:
    out_path = re.sub(r"(?<=\.)flac$", "wav", path)
    subprocess.run(["ffmpeg", "-i", path, out_path])
    return True

def make_wavs(path) -> bool:
    for _f in list_flacs(path):
        flac2wav(_f)
    return True

def rm_flacs(path) -> bool:
    for _f in list_flacs(path):
        shutil.os.remove(_f)

