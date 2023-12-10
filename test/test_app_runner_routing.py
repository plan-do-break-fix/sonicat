
import pytest

from apps.AppRunner import AppRunner

@pytest.fixture
def apprunner():
    return AppRunner(debug=True)

DISCOGS_TASK = {"app_name": "discogs"}
FILE_MOVER_TASK = {"app_name": "file_mover"}
INVENTORY_TASK = {"app_name": "inventory"}
LASTFM_TASK = {"app_name": "lastfm"}
LIBROSA_TASK = {"app_name": "librosa"}
PATH_PARSER_TASK = {"app_name": "path_parser"}

# TEMPLATE
'''
@pytest.mark.parametrize(
    "task, app_name, app_type, expected",
    [
        ()
    ]
)
def test_route_target_template(task, app_name, app_type, expected, apprunner):
    assert apprunner.route_target(task,
                                  routing_app_name=app_name,
                                  routing_app_type=app_type
                                  ) == expected
'''

# Testing by task lifecycles


## Discogs Task
@pytest.mark.parametrize(
    "app_name, app_type, expected",
    [
        ("tasks", "system", "discogs"),
        ("discogs", "metadata", "app_data"),
        ("app_data", "system", "")
    ]
)
def test_route_target_discogs(app_name, app_type, expected, apprunner):
    assert apprunner.route_target(DISCOGS_TASK,
                                  routing_app_name=app_name,
                                  routing_app_type=app_type
                                  ) == expected
## File Mover Task
@pytest.mark.parametrize(
    "app_name, app_type, expected",
    [
        ("tasks", "system", "file_mover"),
        ("file_mover", "system", "tasks")
    ]
)
def test_route_target_file_mover(app_name, app_type, expected, apprunner):
    assert apprunner.route_target(FILE_MOVER_TASK,
                                  routing_app_name=app_name,
                                  routing_app_type=app_type
                                  ) == expected
## Inventory Task
@pytest.mark.parametrize(
    "app_name, app_type, expected",
    [
        ("tasks", "system", "inventory"),
        ("inventory", "system", "app_data"),
        ("app_data", "system", "file_mover"),
        ("file_mover", "system", "tasks")
    ]
)
def test_route_target_inventory(app_name, app_type, expected, apprunner):
    assert apprunner.route_target(INVENTORY_TASK,
                                  routing_app_name=app_name,
                                  routing_app_type=app_type
                                  ) == expected
## Lastfm Task
@pytest.mark.parametrize(
    "app_name, app_type, expected",
    [
        ("tasks", "system", "lastfm"),
        ("lastfm", "metadata", "app_data"),
        ("app_data", "system", "")
    ]
)
def test_route_target_lastfm(app_name, app_type, expected, apprunner):
    assert apprunner.route_target(LASTFM_TASK,
                                  routing_app_name=app_name,
                                  routing_app_type=app_type
                                  ) == expected
## Librosa Task
@pytest.mark.parametrize(
    "app_name, app_type, expected",
    [
        ("tasks", "system", "librosa"),
        ("librosa", "analysis", "app_data"),
        ("app_data", "system", "file_mover"),
        ("file_mover", "system", "tasks")
    ]
)
def test_route_target_librosa(app_name, app_type, expected, apprunner):
    assert apprunner.route_target(LIBROSA_TASK,
                                  routing_app_name=app_name,
                                  routing_app_type=app_type
                                  ) == expected




# Routing from Tasks App
@pytest.mark.parametrize(
    "task, expected",
    [
        (FILE_MOVER_TASK, "file_mover"),
        (DISCOGS_TASK, "discogs"),
        (LASTFM_TASK, "lastfm")
    ]
)
def test_route_target_dispatch(task, expected, apprunner):
    assert apprunner.route_target(task,
                                  routing_app_name="tasks",
                                  routing_app_type="system"
                                  ) == expected

# Task routing is complete
@pytest.mark.parametrize(
    "task, app_name, app_type",
    [
        (DISCOGS_TASK, "app_data", "system"),
        (LASTFM_TASK, "app_data", "system")
    ]
)
def test_route_target_terminal(task, app_name, app_type, apprunner):
    assert apprunner.route_target(task,
                                  routing_app_name=app_name,
                                  routing_app_type=app_type
                                  ) == ""
    
# AppData after recording primary analysis results
def test_route_target_primary_analysis(apprunner):
    assert apprunner.route_target(LIBROSA_TASK,
                                  routing_app_name="app_data",
                                  routing_app_type="system"
                                  ) == "file_mover"