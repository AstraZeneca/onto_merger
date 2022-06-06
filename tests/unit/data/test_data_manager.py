"""Tests for the DataManager class."""
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from onto_merger.data.constants import (
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_DROPPED_MAPPINGS,
    DIRECTORY_INPUT,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_LOGS,
    DIRECTORY_OUTPUT,
    DIRECTORY_PROFILED_DATA,
    DIRECTORY_REPORT,
    FILE_NAME_LOG,
    SCHEMA_HIERARCHY_EDGE_TABLE,
    SCHEMA_MAPPING_TABLE,
    SCHEMA_MERGE_TABLE,
    SCHEMA_MERGE_TABLE_WITH_META_DATA,
    TABLE_EDGES_HIERARCHY,
    TABLE_MAPPINGS,
    TABLE_MERGES,
    TABLE_MERGES_WITH_META_DATA,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import AlignmentConfig, NamedTable
from tests.fixtures import TEST_FOLDER_OUTPUT_PATH, data_manager


@pytest.fixture()
def loaded_table_mappings() -> NamedTable:
    return NamedTable(
        TABLE_MAPPINGS,
        pd.DataFrame(
            [("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO")],
            columns=SCHEMA_MAPPING_TABLE,
        ),
    )


def test_load_table(data_manager: DataManager):
    actual = data_manager.load_table(table_name=TABLE_MAPPINGS, process_directory=DIRECTORY_INPUT)
    assert isinstance(actual, DataFrame)
    assert len(actual) > 0


def test_convert_config_json_to_dataclass(data_manager: DataManager):
    config_json_dic = {
        "domain_node_type": "Disease",
        "seed_ontology_name": "MONDO",
        "required_full_hierarchies": ["MONDO", "ORPHANET", "MEDDRA"],
        "mappings": {
            "type_groups": {
                "equivalence": [
                    "equivalent_to",
                    "merge",
                    "alexion_orphanet_omim_exact",
                ],
                "database_reference": ["database_cross_reference", "xref"],
                "label_match": [],
            }
        },
    }
    actual = data_manager.convert_config_json_to_dataclass(config_json=config_json_dic)
    assert isinstance(actual, AlignmentConfig)
    assert isinstance(actual.as_dict, dict)


def test_get_absolute_path(data_manager: DataManager):
    file_path_relative = os.path.join("../unit/test_data", "foo_report.html")
    expected = os.path.abspath(file_path_relative)
    actual = data_manager.get_absolute_path(file_path_relative)
    assert isinstance(actual, str)
    assert actual == expected


def test_get_profiled_table_report_path(data_manager: DataManager):
    file_name = "foo_report.html"

    # absolute path
    expected_abs = os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_REPORT, DIRECTORY_PROFILED_DATA, file_name)
    actual_abs = data_manager.get_profiled_table_report_path(table_name="foo", relative_path=False)
    assert isinstance(actual_abs, str)
    assert actual_abs == expected_abs

    # relative path
    expected_rel = os.path.join(DIRECTORY_PROFILED_DATA, file_name)
    actual_rel = data_manager.get_profiled_table_report_path(table_name="foo", relative_path=True)
    assert isinstance(actual_rel, str)
    assert actual_rel == expected_rel


def test_get_log_file_path(data_manager: DataManager):
    expected = os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_REPORT, DIRECTORY_LOGS, FILE_NAME_LOG)
    actual = data_manager.get_log_file_path()
    assert isinstance(actual, str)
    assert actual == expected


def test_save_tables(data_manager: DataManager, loaded_table_mappings: NamedTable):
    data_manager.save_tables(tables=[loaded_table_mappings])
    expected_path = os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_INTERMEDIATE, "mappings.csv")
    assert os.path.exists(expected_path) is True
    assert os.path.isfile(expected_path) is True
    assert os.stat(expected_path).st_size > 0
    Path(expected_path).unlink()


def test_save_table(data_manager: DataManager, loaded_table_mappings: NamedTable):
    data_manager.save_table(table=loaded_table_mappings)
    expected_path = os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_INTERMEDIATE, "mappings.csv")
    assert os.path.exists(expected_path) is True
    assert os.path.isfile(expected_path) is True
    assert os.stat(expected_path).st_size > 0
    Path(expected_path).unlink()

    data_manager.save_table(table=loaded_table_mappings,
                            process_directory=f"{DIRECTORY_OUTPUT}/{DIRECTORY_DOMAIN_ONTOLOGY}")
    expected_path2 = os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_DOMAIN_ONTOLOGY, "mappings.csv")
    assert os.path.exists(expected_path2) is True
    assert os.path.isfile(expected_path2) is True
    assert os.stat(expected_path2).st_size > 0
    Path(expected_path2).unlink()


def test_save_dropped_mappings_table(data_manager: DataManager):
    test_folder_intermediate_dropped_mappings = os.path.join(
        TEST_FOLDER_OUTPUT_PATH,
        DIRECTORY_INTERMEDIATE,
        DIRECTORY_DROPPED_MAPPINGS
    )

    table_1 = pd.DataFrame(
        [("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO")],
        columns=SCHEMA_MAPPING_TABLE,
    )
    data_manager.save_dropped_mappings_table(table=table_1, step_count=1, source_id="FOO", mapping_type="equivalent_to")
    assert os.path.exists(
        os.path.join(
            test_folder_intermediate_dropped_mappings,
            "equivalent_to_1_FOO.csv",
        )
    )

    table_2 = pd.DataFrame(
        [],
        columns=SCHEMA_MAPPING_TABLE,
    )
    data_manager.save_dropped_mappings_table(table=table_2, step_count=2, source_id="BAR", mapping_type="equivalent_to")
    assert (
            os.path.exists(
                os.path.join(
                    test_folder_intermediate_dropped_mappings,
                    "equivalent_to_2_BAR.csv",
                )
            )
            is False
    )


def test_merge_tables(loaded_table_mappings: NamedTable):
    table_2 = NamedTable(
        TABLE_MAPPINGS,
        pd.DataFrame(
            [("MONDO:0000005", "MONDO:0000999", "equivalent_to", "MONDO")],
            columns=SCHEMA_MAPPING_TABLE,
        ),
    )
    expected = NamedTable(
        TABLE_MAPPINGS,
        pd.DataFrame(
            [
                ("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO"),
                ("MONDO:0000005", "MONDO:0000999", "equivalent_to", "MONDO"),
            ],
            columns=SCHEMA_MAPPING_TABLE,
        ),
    )
    actual = DataManager.merge_tables_of_same_type(tables=[loaded_table_mappings, table_2])
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert actual.name == TABLE_MAPPINGS
    assert np.array_equal(actual.dataframe.values, expected.dataframe.values) is True


def test_produce_empty_merge_table():
    expected = pd.DataFrame([], columns=SCHEMA_MERGE_TABLE_WITH_META_DATA)
    actual = DataManager.produce_empty_merge_table()
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert actual.name == TABLE_MERGES_WITH_META_DATA
    assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_empty_hierarchy_table():
    expected = pd.DataFrame([], columns=SCHEMA_HIERARCHY_EDGE_TABLE)
    actual = DataManager.produce_empty_hierarchy_table()
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert actual.name == TABLE_EDGES_HIERARCHY
    assert np.array_equal(actual.dataframe.values, expected.values) is True
