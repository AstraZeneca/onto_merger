"""Tests for the Pipeline class."""
import os
import shutil
from ast import literal_eval
from pathlib import Path

import pandas as pd
import pytest

from onto_merger.data.constants import (
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_OUTPUT,
    DIRECTORY_REPORT,
)
from onto_merger.pipeline.pipeline import Pipeline
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
        "analysis",
        "alignment_steps_report.csv",
        "connectivity_steps_report.csv",
        "edges_hierarchy_post.csv",
        "mappings_for_input_nodes.csv",
        "mappings_obsolete_to_current.csv",
        "mappings_updated.csv",
        "merges_with_meta_data.csv",
        "merges_aggregated.csv",
        "nodes_dangling.csv",
        "nodes_connected.csv",
        "nodes_connected_excluding_seed.csv",
        "nodes_merged_to_other.csv",
        "nodes_merged_to_seed.csv",
        "nodes_seed.csv",
        "nodes_unmapped.csv",
        "nodes_merged.csv",
        "pipeline_steps_report.csv",
    }
    actual_outputs_intermediate = set(os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_INTERMEDIATE)))
    assert len(actual_outputs_intermediate) > 0
    assert actual_outputs_intermediate == expected_outputs_intermediate

    # report
    expected_outputs_report = {
        "data_docs",
        "data_profile_reports",
        "images",
        "logs",
        "index.html",
    }
    actual_outputs_report = set(os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_REPORT)))
    assert len(actual_outputs_report) > 0
    assert actual_outputs_report == expected_outputs_report
    check_report_data()

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


def check_report_data() -> None:
    test_file_lists_path = "../onto_merger/tests/integration"

    # check report
    report_path = os.path.abspath(os.path.join(
        TEST_FOLDER_OUTPUT_PATH, "report/index.html",
    ))
    assert os.path.getsize(report_path) > 100

    # check figures
    analysis_figure_folder_path = os.path.join(
        TEST_FOLDER_OUTPUT_PATH, "report", "images"
    )
    expected_file_list = pd.read_csv(os.path.abspath(
        os.path.join(f'{test_file_lists_path}/image_and_figure_files.csv')))['file'].tolist()
    actual_file_list = [path for path in os.listdir(Path(analysis_figure_folder_path))
                        if Path(os.path.join(analysis_figure_folder_path, path)).is_file()
                        and str(path).endswith(".png")]
    # assert set(actual_file_list) == set(expected_file_list) todo
    for actual_file in actual_file_list:
        assert os.path.getsize(os.path.join(analysis_figure_folder_path, actual_file)) > 10

    # check tables (length, columns)
    expected_table_files_df = pd.read_csv(
        os.path.abspath(os.path.join(f'{test_file_lists_path}/analysis_table_files.csv')),
        converters={"table_columns": literal_eval}
    )
    expected_table_paths = expected_table_files_df['analysis_table_file'].tolist()
    analysis_files_folder_path = os.path.join(
        TEST_FOLDER_OUTPUT_PATH, "intermediate", "analysis"
    )
    actual_table_file_paths = [path for path in os.listdir(Path(analysis_files_folder_path))
                               if Path(os.path.join(analysis_files_folder_path, path)).is_file()
                               and (str(path).endswith(".csv") and "top10" not in str(path))]

    assert len(actual_table_file_paths) >= len(expected_table_paths)  # todo
    for _, row in expected_table_files_df.iterrows():
        table_path = row['analysis_table_file']
        table_columns = row['table_columns']
        actual_path = os.path.join(analysis_files_folder_path, table_path)
        actual_df = pd.read_csv(actual_path)
        assert table_path in actual_table_file_paths
        assert os.path.getsize(actual_path) > 10
        assert len(actual_df) > 0
        missing_elements = [col_name for col_name in table_columns if col_name not in list(actual_df)]
        assert len(missing_elements) == 0
