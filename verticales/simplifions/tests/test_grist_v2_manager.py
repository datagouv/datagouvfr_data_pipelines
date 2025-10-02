from unittest.mock import Mock, patch
import time

# The factory must be imported before the manager because it initializes the mocks
from factories.grist_factory import GristFactory
from grist_v2_manager import GristV2Manager

grist_v2_manager = GristV2Manager()
grist_factory = GristFactory()


def test_clean_row():
    row = {
        "id": 1,
        "fields": {
            "key": "value",
            "list_key": ["L", "1", "2", "3"],
            "list_key_2": ["L", "A", "B", "C"],
        },
    }
    cleaned_row = grist_v2_manager._clean_row(row)
    assert cleaned_row == {
        "id": 1,
        "fields": {
            "key": "value",
            "list_key": ["1", "2", "3"],
            "list_key_2": ["A", "B", "C"],
        },
    }


# Tests for moved methods are now in test_task_functions.py
