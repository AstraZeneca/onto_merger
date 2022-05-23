import os
import pytest

from onto_merger.data.data_manager import DataManager
from onto_merger.report.report_generator import produce_report
from tests.fixtures import (
    TEST_FOLDER_OUTPUT_PATH,
    data_manager,
)


def test_produce_report(
        data_manager: DataManager,
) -> None:
    # todo ensure analysis files exit
    expected_file_path = data_manager.produce_analysis_report_path()
    actual_file_path = produce_report(data_manager=data_manager)
    assert isinstance(actual_file_path, str)
    assert actual_file_path == expected_file_path
    assert os.path.exists(actual_file_path)
    assert os.path.isfile(actual_file_path) is True
    assert os.stat(actual_file_path).st_size > 100
