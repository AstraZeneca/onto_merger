"""Tests for the GE runner class."""

import os

from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import AlignmentConfig
from onto_merger.data_testing.ge_runner import GERunner
from tests.fixtures import alignment_config, data_manager, ge_test_folder_path


def test_run_ge_tests_empty(alignment_config: AlignmentConfig, ge_test_folder_path: str):
    GERunner(
        alignment_config=alignment_config,
        ge_base_directory=ge_test_folder_path,
        data_manager=data_manager
    ) \
        .run_ge_tests(named_tables=[], data_origin="FOO")
    assert len(os.listdir(os.path.join(ge_test_folder_path, "expectations"))) == 1
    assert len(os.listdir(os.path.join(ge_test_folder_path, "checkpoints"))) == 0
    assert len(os.listdir(os.path.join(ge_test_folder_path, "uncommitted/validations"))) == 1


def test_run_ge_tests(
        alignment_config: AlignmentConfig,
        ge_test_folder_path: str,
        data_manager: DataManager,
):
    GERunner(
        alignment_config=alignment_config,
        ge_base_directory=ge_test_folder_path,
        data_manager=data_manager
    ) \
        .run_ge_tests(
        named_tables=data_manager.load_input_tables()[0:1],
        data_origin="FOO"
    )
    assert len(os.listdir(os.path.join(ge_test_folder_path, "expectations"))) > 1
    assert len(os.listdir(os.path.join(ge_test_folder_path, "checkpoints"))) == 1
    assert len(os.listdir(os.path.join(ge_test_folder_path, "uncommitted/validations"))) > 1
    assert len(os.listdir(os.path.join(ge_test_folder_path, "uncommitted/data_docs/local_site"))) > 1
