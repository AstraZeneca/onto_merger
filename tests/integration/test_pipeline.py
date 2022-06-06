"""Tests for the Pipeline class."""
import os
import shutil
import pytest
from pathlib import Path

import pandas as pd

from onto_merger.data import DataManager
from onto_merger.data.constants import (
    DIRECTORY_DATA_TESTS,
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_DROPPED_MAPPINGS,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_OUTPUT,
    DIRECTORY_REPORT,
)
from onto_merger.pipeline import Pipeline
from tests.fixtures import TEST_FOLDER_OUTPUT_PATH, TEST_FOLDER_PATH, data_manager


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
        "merges_with_meta_data.csv",
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
    test_produce_report_data(data_manager=data_manager)

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


def test_produce_report_data(
        data_manager: DataManager
) -> None:
    # check report
    report_path = os.path.join(
        data_manager.produce_analysis_report_path(),
    )
    assert os.path.getsize(report_path) > 100

    # check figures
    analysis_figures_df = pd.read_csv('analysis_figures.csv')
    analysis_figure_folder_path = os.path.join(
        data_manager.get_output_report_folder_path(),
        "images"
    )
    expected_file_list = analysis_figures_df['analysis_figure'].tolist()
    actual_file_list = [path for path in os.listdir(Path(analysis_figure_folder_path)) if path.is_file()]
    assert set(actual_file_list) == set(expected_file_list)
    for actual_file in actual_file_list:
        assert os.path.getsize(os.path.join(analysis_figure_folder_path, actual_file)) > 10

    # check tables
    expected_table_files_df = pd.read_csv('analysis_table_files.csv')
    expected_table_paths = expected_table_files_df['analysis_table_file'].tolist()
    analysis_files_folder_path = data_manager.get_analysis_folder_path()
    actual_table_file_paths = [path for path in os.listdir(Path(analysis_files_folder_path)) if path.is_file()]
    assert len(actual_table_file_paths) == len(expected_table_paths)
    for actual_table_file in actual_table_file_paths:
        assert actual_table_file in expected_table_paths
        actual_path = os.path.join(analysis_files_folder_path, actual_table_file)
        assert os.path.getsize(actual_path) > 10
        actual_df = pd.read_csv(actual_path)
        assert len(actual_df) > 0
        assert set(actual_df.tolist()) == set(expected_table_files_df.tolist())
