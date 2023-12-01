import pytest

from util.NameUtility import NameUtility, Variations


@pytest.fixture
def utility():
    return NameUtility()

@pytest.fixture
def variations():
    return Variations()

@pytest.mark.parametrize(
    "input, expected",
    [
        ("MSXII Sound - Lofi Jazz Guitar 2", "msxii_sound"),
        ("Past to Future Samples - 12-Bit Hip-Hop Drums", "past_to_future_samples"),
        ("789ten - The Jaxx & Vega Ultimate Big Room Pack 1", "789ten"),
        ("Splice Sounds - Unmüte - Cosmos", "splice_sounds"),
        ("Splice Sounds - VÉRITÉ - New Noise Sample Pack", "splice_sounds"),
    ]
)
def test_pub_dir_from_cname(input, expected, utility):
    assert utility.label_dir_from_cname(input) == expected


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
def test_name_is_canonical(input, expected, utility):
    assert utility.name_is_canonical(input) == expected



