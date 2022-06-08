import json
import os
import shutil
import typing
from pathlib import Path
from typing import List

import pytest

from onto_merger.alignment import AlignmentManager
from onto_merger.analyser.analysis_utils import (
    produce_table_with_namespace_column_for_node_ids,
)
from onto_merger.data.constants import (
    DIRECTORY_DATA_TESTS,
    DIRECTORY_OUTPUT,
    FILE_NAME_CONFIG_JSON,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import AlignmentConfig, DataRepository, NamedTable

TEST_FOLDER_PATH = os.path.abspath("./tests/test_data")
TEST_FOLDER_OUTPUT_PATH = os.path.abspath(f"./tests/test_data/{DIRECTORY_OUTPUT}")
TEST_FOLDER_GE = os.path.abspath(f"./tests/test_data/{DIRECTORY_OUTPUT}")


@typing.no_type_check
@pytest.fixture()
def data_repo() -> DataRepository:
    data_repo = DataRepository()
    preprocessed_tables = [
        NamedTable(
            name=table.name,
            dataframe=produce_table_with_namespace_column_for_node_ids(table.dataframe),
        )
        for table in DataManager(project_folder_path=TEST_FOLDER_PATH).load_input_tables()
    ]
    data_repo.update(tables=preprocessed_tables)
    return data_repo


@typing.no_type_check
@pytest.fixture()
def data_manager() -> DataManager:
    if os.path.exists(TEST_FOLDER_OUTPUT_PATH) & os.path.isdir(TEST_FOLDER_OUTPUT_PATH):
        shutil.rmtree(TEST_FOLDER_OUTPUT_PATH)
    yield DataManager(project_folder_path=TEST_FOLDER_PATH)
    if os.path.exists(TEST_FOLDER_OUTPUT_PATH) & os.path.isdir(TEST_FOLDER_OUTPUT_PATH):
        shutil.rmtree(TEST_FOLDER_OUTPUT_PATH)


@typing.no_type_check
@pytest.fixture()
def alignment_config() -> AlignmentConfig:
    config_json = os.path.join(TEST_FOLDER_PATH, "input", FILE_NAME_CONFIG_JSON)
    with open(config_json) as json_file:
        config_json = json.load(json_file)
    yield DataManager.convert_config_json_to_dataclass(config_json=config_json)


@typing.no_type_check
@pytest.fixture()
def ge_test_folder_path() -> str:
    path = os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_DATA_TESTS)
    Path(path).mkdir(parents=True, exist_ok=True)
    yield path
    if os.path.exists(path) & os.path.isdir(path):
        shutil.rmtree(path)


@pytest.fixture()
def source_alignment_priority_order() -> List[str]:
    return [
        "MONDO",
        "MEDDRA",
        "ORPHANET",
        "MESH",
        "ICD10CM",
        "EFO",
        "DOID",
        "SNOMED"
    ]
