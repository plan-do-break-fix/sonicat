import pytest

from util.NameUtility import Validate, Transform


@pytest.fixture
def validate():
    return Validate()

@pytest.fixture
def transform():
    return Transform()


@pytest.mark.parametrize(
    "input, expected",
    [
        ("MSXII Sound - Lofi Jazz Guitar 2", True),
        ("Past to Future Samples - 12-Bit Hip-Hop Drums", True),
        ("789ten - The Jaxx & Vega Ultimate Big Room Pack 1", True),
        ("Splice Sounds - Unmüte - Cosmos", True),
        ("Splice Sounds - VÉRITÉ - New Noise Sample Pack", True),
        ("Montage by Splice Light Refractions Celestial Ambient", False),
        ("Omega Music Library 4 (Compositions and Stems)", False),
        ("The Best of City Pop", False),
        ("Convexity Data Cartridge", False),
        ("Gothic Storm Music", False),
    ]
)
def test_name_is_canonical(input, expected, validate):
    assert validate.name_is_canonical(input) == expected



@pytest.mark.parametrize(
    "input, expected",
    [
        ("Wave Alchemy", ["wave", "alchemy"]),
        ("Cymatics", ["cymatics"]),
        ("Cymatics - Gems 14 - Lofi", ["cymatics", "gems", "14", "lofi"])
    ]
)
def test_tokenize(input, expected, transform):
    assert transform.tokenize(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (["wave", "alchemy"], ["wavealchemy", "wave-alchemy"]),
        (["cymatics"], ["cymatics"]),
        (["cymatics", "gems", "14", "lofi"], ["cymaticsgems14lofi",
                                              "cymatics-gems-14-lofi"])
    ]
)
def test_join_tokens(input, expected, transform):
    assert transform.join_tokens(input) == expected




@pytest.mark.parametrize(
    "input, expected",
    [
        ("Wave Alchemy", ["wavealchemy", "wave-alchemy"]),
        ("Cymatics", ["cymatics"]),
        ("Cymatics - Gems 14 - Lofi",
            ["cymatics",
            "gems14", "gems-14",
            "lofi",
            "cymaticsgems14lofi",
            "cymatics-gems-14-lofi"
            ]
        )
    ]
)
def test_name_forms(input, expected, transform):
    result = transform.name_forms(input)
    result.sort(), expected.sort()
    assert result == expected

