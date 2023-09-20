import pytest

from util.FileUtility import FileUtility


#@pytest.fixture
#def file_utility():
#    return Handler(log_path="/tmp/testlog.log")


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
def test_pub_dir_from_cname(input, expected):
    assert FileUtility.pub_dir_from_cname(input) == expected


  