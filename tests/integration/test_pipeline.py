"""Tests for the Pipeline class."""
import os
import shutil

import pytest

from onto_merger.data.constants import (
    DIRECTORY_DATA_TESTS,
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_DROPPED_MAPPINGS,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_OUTPUT,
    DIRECTORY_REPORT,
)
from onto_merger.pipeline import Pipeline
from tests.fixtures import TEST_FOLDER_OUTPUT_PATH, TEST_FOLDER_PATH


def perform_evaluation_for_pipeline_run():
    assert os.path.exists(TEST_FOLDER_OUTPUT_PATH) is True

    # domain ontology
    expected_outputs_domain = {
        "merges.csv",
        "mappings.csv",
        "edges_hierarchy.csv",
        "nodes.csv",
    }
    actual_outputs_domain = set(os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_DOMAIN_ONTOLOGY)))
    assert len(actual_outputs_domain) > 0
    assert actual_outputs_domain == expected_outputs_domain

    # intermediate
    expected_outputs_intermediate = {
        "data_tests",
        "dropped_mappings",
        "alignment_steps_report.csv",
        "connectivity_steps_report.csv",
        "edges_hierarchy_post.csv",
        "mappings_obsolete_to_current.csv",
        "mappings_updated.csv",
        "merges.csv",
        "merges_aggregated.csv",
        "nodes_dangling.csv",
        "nodes_merged.csv",
        "nodes_only_connected.csv",
        "nodes_unmapped.csv",
    }
    actual_outputs_intermediate = set(os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_INTERMEDIATE)))
    assert len(actual_outputs_intermediate) > 0
    assert actual_outputs_intermediate == expected_outputs_intermediate

    # report
    expected_outputs_report = {
        "data_docs",
        "data_profile_reports",
        "logs",
        "index.html",
    }
    actual_outputs_report = set(os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_REPORT)))
    assert len(actual_outputs_report) > 0
    assert actual_outputs_report == expected_outputs_report

    shutil.rmtree(TEST_FOLDER_OUTPUT_PATH)


def test_run_alignment_and_connection_process():
    assert os.path.exists(TEST_FOLDER_OUTPUT_PATH) is False

    Pipeline(project_folder_path=TEST_FOLDER_PATH).run_alignment_and_connection_process()

    perform_evaluation_for_pipeline_run()


def test_run_alignment_and_connection_process_invalid():
    test_folder_invalid = os.path.abspath("../test_data_invalid")
    test_folder_invalid_output = os.path.abspath(f"../test_data_invalid/{DIRECTORY_OUTPUT}")

    with pytest.raises(Exception):
        Pipeline(project_folder_path=test_folder_invalid).run_alignment_and_connection_process()

    assert os.path.exists(test_folder_invalid_output) is True
    expected_outputs = {
        DIRECTORY_DOMAIN_ONTOLOGY,
        DIRECTORY_INTERMEDIATE,
        DIRECTORY_REPORT,
    }
    actual_outputs = set(os.listdir(test_folder_invalid_output))
    assert len(actual_outputs) > 0
    assert actual_outputs == expected_outputs
    shutil.rmtree(test_folder_invalid_output)
