"""Tests for the Pipeline class."""
import os
import shutil

import pytest

from onto_merger.data.constants import (
    DIRECTORY_DATA_TESTS,
    DIRECTORY_DROPPED_MAPPINGS,
    DIRECTORY_OUTPUT,
    DIRECTORY_REPORT,
)
from onto_merger.pipeline import Pipeline
from tests.fixtures import TEST_FOLDER_OUTPUT_PATH, TEST_FOLDER_PATH


def perform_evaluation_for_pipeline_run():
    assert os.path.exists(TEST_FOLDER_OUTPUT_PATH) is True
    expected_outputs = {
        "merges_aggregated.csv",
        "merges.csv",
        "nodes_unmapped.csv",
        "mappings_obsolete_to_current.csv",
        "data_tests",
        "nodes_merged.csv",
        "mappings_updated.csv",
        "onto-merger.logger",
        "alignment_steps_report.csv",
        "nodes_only_connected.csv",
        "report",
        "edges_hierarchy_post.csv",
        "dropped_mappings",
        "nodes_dangling.csv",
    }
    actual_outputs = set(os.listdir(TEST_FOLDER_OUTPUT_PATH))
    assert len(actual_outputs) > 0
    assert actual_outputs == expected_outputs
    shutil.rmtree(TEST_FOLDER_OUTPUT_PATH)


def test_run_alignment_and_connection_process():
    assert os.path.exists(TEST_FOLDER_OUTPUT_PATH) is False

    Pipeline(project_folder_path=TEST_FOLDER_PATH).run_alignment_and_connection_process()

    perform_evaluation_for_pipeline_run()


def test_run_alignment_and_connection_process_invalid():
    test_folder_invalid = os.path.abspath("../test_data_invalid")
    test_folder_invalid_output = os.path.abspath(f"../test_data_invalid/{DIRECTORY_OUTPUT}")

    assert os.path.exists(test_folder_invalid_output) is False

    with pytest.raises(Exception):
        Pipeline(project_folder_path=test_folder_invalid).run_alignment_and_connection_process()

    assert os.path.exists(test_folder_invalid_output) is True
    expected_outputs = {
        DIRECTORY_DATA_TESTS,
        DIRECTORY_REPORT,
        DIRECTORY_DROPPED_MAPPINGS,
    }
    actual_outputs = set(os.listdir(test_folder_invalid_output))
    assert len(actual_outputs) > 0
    assert actual_outputs == expected_outputs
    shutil.rmtree(test_folder_invalid_output)
