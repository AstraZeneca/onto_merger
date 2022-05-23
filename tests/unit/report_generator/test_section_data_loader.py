import os
from pathlib import Path

import pandas as pd
import pytest

from onto_merger.data.data_manager import DataManager
from onto_merger.report import section_data_loader
from tests.fixtures import (
    TEST_FOLDER_OUTPUT_PATH,
    data_manager,
)


def test_load_report_data(
        data_manager: DataManager,
) -> None:
    expected_content_keys = [
        "date", "version", "title", "overview_data", "input_data", "output_data", "alignment_data",
        "connectivity_data", "data_profiling", "data_tests"
     ]
    actual_report_data = section_data_loader.load_report_data(data_manager=data_manager)
    assert isinstance(actual_report_data, dict)
    for key in expected_content_keys:
        assert key in actual_report_data
        assert actual_report_data.get(key) != None
