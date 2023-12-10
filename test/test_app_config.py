import pytest

from apps.App import AppConfig

@pytest.fixture
def config():
    test_config = {
        "catalogs": {
            "catalog1": {},
            "catalog2": {}
        },
        "apps": {
            "test": {
                "debug": {
                    "moniker": "TestApp",
                    "log_level": "debug"
                }
            },
            "analysis": {
                "test_anal": {}
            },
            "tokens": {
                "test_tokens": {},
                "test_tags": {}
            }
        }
    }
    return AppConfig("/test", "debug", debug_cfg=test_config)

@pytest.mark.parametrize(
    "attribute, expected",
    [
        ("sonicat_path", "/test"),
        ("app_name", "debug"),
        ("app_type", "test"),
        ("app_moniker", "TestApp"),
        ("log_level", "debug")
    ]
)
def test_init(attribute, expected, config):
    assert config.__getattribute__(attribute) == expected

@pytest.mark.parametrize(
    "input, expected",
    [
        ("debug", "test"),
        ("test_anal", "analysis"),
        ("test_tokens", "tokens"),
        ("test_tags", "tokens")
    ]
)
def test_type_of_app(input, expected, config):
    assert config.type_of_app(input) == expected

def test_catalog_names(config):
    _names = config.catalog_names()
    _names.sort()
    assert _names == ["catalog1", "catalog2"]

@pytest.mark.parametrize(
        "input, expected",
        [
            (["test"], ["debug"]),
            (["analysis"], ["test_anal"]),
            (["tokens"], ["test_tags", "test_tokens"]),
            ([], ["debug", "test_anal", "test_tags", "test_tokens"]),
            (["test", "analysis"], ["debug", "test_anal"])
        ]
)
def test_app_names(input, expected, config):
    _names = config.app_names(app_types=input)
    _names.sort()
    assert _names == expected

def test_data_path(config):
    assert config.data_path() == "/test/data/test"

def test_log_path(config):
    assert config.log_path() == "/test/log/test"

def test_temp_path(config):
    assert config.temp_path() == "/tmp/sonicat-TestApp"

