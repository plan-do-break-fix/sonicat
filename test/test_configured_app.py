import pytest

from apps.ConfiguredApp import App

@pytest.fixture
def app():
    app = App("test/test_data/test_configs", "test")
    return app


@pytest.mark.parametrize(
    "attr_name, expected",
    [
        ("name", "TCat"),
        ("log_level", "debug"),
        ("data", "test/test_data/test_configs/catalog/data"),
        ("managed", "/tmp/sonicat_test/managed"),
        ("log", "test/test_data/test_configs/catalog/log"),
        ("intake", "/tmp/sonicat_test/intake"),
        ("export", "/tmp/sonicat_test/out"),
        ("temp", "/tmp")
    ]
)
def test_config_file_load(attr_name, expected, app):
    assert app.cfg.__getattribute__(attr_name) == expected
