import os
from pathlib import Path

import pandas as pd
import pytest

from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import AlignmentConfig, DataRepository
from onto_merger.analyser.report_analyser import ReportAnalyser
from tests.fixtures import (
    TEST_FOLDER_OUTPUT_PATH,
    alignment_config,
    data_manager,
    data_repo,
    source_alignment_priority_order,
)


def test_produce_report_data(
        alignment_config: AlignmentConfig,
        data_repo: DataRepository,
        data_manager: DataManager,
) -> None:

    # todo ensure data exits

    # produce analysis
    ReportAnalyser(alignment_config=alignment_config,
                   data_manager=data_manager,
                   data_repo=data_repo).produce_report_data()

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
