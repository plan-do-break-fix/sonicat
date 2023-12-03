import pytest

from util.Parser import AudioFilePathParser

@pytest.fixture
def parser():
    return AudioFilePathParser()


@pytest.mark.parametrize(
    "input, expected",
    [
        ("  REVRSP1    Vocal Shots/REVRSP1 Arsen Vocal Shot  05  A# ",
         "REVRSP1 Vocal Shots REVRSP1 Arsen Vocal Shot 05 A#"),
        ("REVRSP1 Vocal Shots/REVRSP1 Arsen Vocal Shot 05 A#",
         "REVRSP1 Vocal Shots REVRSP1 Arsen Vocal Shot 05 A#"),
        ("ambience_chordautomate-1", "ambience chordautomate 1"),
        ("APT_Wav Piano Themes_FA/APT_100 BPM_Piano Themes_FA/APT_100_Piano Theme_06_FA",
         "APT Wav Piano Themes FA APT 100 BPM Piano Themes FA APT 100 Piano Theme 06 FA"),
        ("Serious Electro Vol 2_Oneshots_FA/Bass Hits/SE2_F#_Acidline_FA",
         "Serious Electro Vol 2 Oneshots FA Bass Hits SE2 F# Acidline FA"),
        (" Fxlicia - Astral(One Shot Kit)", "Fxlicia Astral One Shot Kit"),
        ("No Defeat - C#min/PLZZDELETE - No Defeat - 137bpm",
         "No Defeat C#min PLZZDELETE No Defeat 137bpm"),
        ("[LEAD] - Voices In My Head", "LEAD Voices In My Head"),
        ("", "")
    ]
)
def test_normal_spaces(input, expected, parser):
    assert parser.normal_spaces(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("Label - Title/subdirectory/example.wav",
         "subdirectory/example"),
        ("Label - Title/subdirectory/example",
         "subdirectory/example"),
        ("example", "example"),
        ("", "")
    ]
)
def test_trim(input, expected, parser):
    assert parser.trim(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("'!?'", ""),
        ("\"Sample Text\"", "Sample Text"),
        ("", "")
    ]
)
def test_cleanse_no_acronyms(input, expected, parser):
    assert parser.cleanse(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("AMAJOR", "amajor"),
        ("am", "am"),
        ("C minor", "c minor"),
        ("a brown fox", "a"),
        ("180BPM C#min", "c#min")
    ]
)
def test_raw_key_signature(input, expected, parser):
    assert parser.raw_key_signature(input.lower()) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("A#m", "A#min"),
        ("A # m", "A#min"),
        ("B Major", "Bmaj"),
        ("bb", "Bb"),
        ("c", "C"),
        ("E MINOR", "Emin"),
        ("G Minor", "Gmin"),
        ("gmin", "Gmin"),
        ("g min", "Gmin"),
        ("c sharp", "C#"),
        ("asharp MINOR", "A#min"),
        ("", "")
    ]
)
def test_normal_key_signature(input, expected, parser):
    assert parser.normal_key_signature(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("example text 100bpm fancy", ["100bpm"]),
        ("bpm110", ["bpm110"]),
        ("110bpm", ["110bpm"]),
        ("bpm 110", ["bpm 110"]),
        ("110 bpm", ["110 bpm"]),
        ("example text", []),
        ("", [])
    ]
)
def test_raw_tempo(input, expected, parser):
    assert parser.raw_tempo_candidates(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("100bpm", "100"),
        ("bpm110", "110"),
        ("110bpm", "110"),
        ("bpm 110", "110"),
        ("110 bpm", "110"),
        ("", "")
    ]
)
def test_normal_tempo(input, expected, parser):
    assert parser.normal_tempo(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("bpm110", ["bpm110"]),
        ("110bpm", ["110bpm"]),
        ("bpm 110", ["bpm 110"]),
        ("110 bpm", ["110 bpm"])
    ]
)
def test_labeled_raw_tempo(input, expected, parser):
    assert parser.labeled_raw_tempo(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("1 Example Track One Cmin 120", ["120"]),
        ("1 Example Track One 120 Cmin", ["120"]),
        ("10 Example Track Ten Cmin 120", ["10", "120"]),
        ("10 Example Track Ten 120 Cmin", ["10", "120"]),
        ("1 10 100 1000", ["10", "100"]),
        ("", [])
    ]
)
def test_unlabeled_raw_tempo(input, expected, parser):
    result = parser.unlabeled_raw_tempo(input)
    result.sort()
    assert result == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (["04", "110bpm"], ("110bpm", "110")),
        (["01 bpm", "bpm 110"], ("bpm 110", "110")),
        (["80", "90"], ("", ""))
    ]
)
def test_tempo_from_candidates(input, expected, parser):
    assert parser.tempo_from_candidates(input) == expected

@pytest.mark.parametrize(
    "input, expected",
    [
        ("Big Label Sounds - Super Audio Loops",
         ["BLS", "SAL", "BLSSAL"]),
        ("Label - Super Audio Loops", ["LSAL", "SAL"]),
        ("Label - Title", ["LT"]),
        ("Label - Title 3", ["LT", "LT3", "LTV3"]),
        ("Label - Multiword Title 2", ["LMT", "MT", "LMT2", "LMTV2", "MT2", "MTV2"])
    ]
)
def test_asset_acronyms(input, expected, parser):
    result = parser.asset_acronyms(input)
    result.sort(), expected.sort()
    assert result == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (["a", "abc", "acc", "b"], ["abc", "acc"]),
        (["test1", "test2"], ["test1", "test2"]),
        (["a", "b", "c"], []),
        ([], [])
    ]
)
def test_drop_tokens_by_len(input, expected, parser):
    assert parser.drop_tokens_by_len(input) == expected

@pytest.mark.parametrize(
    "input, expected",
    [
        (["a", "abc", "acc", "b"], ["abc", "acc"]),
        (["test1", "test2"], ["test1", "test2"]),
        (["aa", "ab", "ac"], ["ab", "ac"]),
        (["aa", "ab", "ca", "cccc"], ["ab", "ca"]),
        ([], [])
    ]
)
def test_drop_spam_tokens(input, expected, parser):
    assert parser.drop_spam_tokens(input) == expected

@pytest.mark.parametrize(
    "input, expected",
    [
        (["a", "abc", "acc", "b"], ["a", "abc", "acc", "b"]),
        (["---===!!!", "test1", "!!!===---"], ["test1"]),
        ([], [])
    ]
)
def test_drop_nonlinguistic_tokens(input, expected, parser):
    assert parser.drop_nonlinguistic_tokens(input) == expected


'''
@pytest.mark.parametrize(
    "input, expected",
    [
        ()
    ]
)
def test_filter_tokens():
    pass
'''