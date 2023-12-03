import pytest
import re

from util import Parser

@pytest.mark.parametrize(
    "input, expected",
    [
        ("AMAJOR", "amajor"),
        ("am", "am"),
        ("C minor", "c minor"),
        ("a brown fox", "a"),
        ("180BPM C#min", "c#min"),
        ("a#2", "a#2"),
        ("gbm", "gbm")

    ]
)
def test_regex_raw_key(input, expected):
    result = re.search(Parser.REGEX_RAW_KEY, input.lower())
    assert result.group() == expected

@pytest.mark.parametrize(
    "input, expected",
    [
        ("120bpm", True),
        ("120 bpm", True),
        ("80BPM", True),
        ("80 BPM", True),
        ("bpm120", False),
        ("bpm 120", False),
        ("BPM80", False),
        ("BPM 80", False)
    ]
)
def test_regex_raw_tempo_postfix(input, expected):
    result = bool(re.search(Parser.REGEX_RAW_TEMPO_POSTFIX, input.lower()))
    assert result == expected

@pytest.mark.parametrize(
    "input, expected",
    [
        ("bpm120", True),
        ("bpm 120", True),
        ("BPM80", True),
        ("BPM 80", True),
        ("120bpm", False),
        ("120 bpm", False),
        ("80BPM", False),
        ("80 BPM", False)
    ]
)
def test_regex_raw_tempo_prefix(input, expected):
    result = bool(re.search(Parser.REGEX_RAW_TEMPO_PREFIX, input.lower()))
    assert result == expected

@pytest.mark.parametrize(
    "input, expected",
    [
        ("01 Track Example 120", ["01", "120"]),
        ("100000", []),
        ("120 Sample Text", ["120"]),
        ("Sample Text 120", ["120"])
    ]
)
def test_regex_raw_tempo_no_label(input, expected):
    result = re.findall(Parser.REGEX_RAW_TEMPO_NO_LABEL, input)
    assert result == expected