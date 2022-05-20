"""Tests for the Profiler class."""

import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from onto_merger.analyser import analysis_utils
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_SOURCE_ID,
    COLUMN_SOURCE_TO_TARGET,
    COLUMN_TARGET_ID,
    SCHEMA_MAPPING_TABLE,
    SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE,
)
from onto_merger.data.dataclasses import NamedTable


@pytest.fixture()
def expected_edge_column_with_node_ids_ns_pair() -> DataFrame:
    return pd.DataFrame(
        [
            (
                "MONDO:0000004",
                "MONDO:0000123",
                "equivalent_to",
                "MONDO",
                "MONDO",
                "MONDO",
                "MONDO to MONDO",
            )
        ],
        columns=SCHEMA_MAPPING_TABLE
        + [
            analysis_utils.get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            analysis_utils.get_namespace_column_name_for_column(COLUMN_TARGET_ID),
            COLUMN_SOURCE_TO_TARGET,
        ],
    )


def test_get_namespace_column_name_for_column():
    actual = analysis_utils.get_namespace_column_name_for_column(node_id_column="foo")
    assert isinstance(actual, str)
    assert actual == "namespace_foo"


def test_get_namespace_for_node_id():
    actual = analysis_utils.get_namespace_for_node_id(node_id="FOO:123")
    assert isinstance(actual, str)
    assert actual == "FOO"


def test_produce_table_with_namespace_column_for_node_ids():
    input_data = pd.DataFrame(
        [("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO")],
        columns=SCHEMA_MAPPING_TABLE,
    )
    expected = pd.DataFrame(
        [
            (
                "MONDO:0000004",
                "MONDO:0000123",
                "equivalent_to",
                "MONDO",
                "MONDO",
                "MONDO",
            )
        ],
        columns=SCHEMA_MAPPING_TABLE
        + [
            analysis_utils.get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            analysis_utils.get_namespace_column_name_for_column(COLUMN_TARGET_ID),
        ],
    )
    actual = analysis_utils.produce_table_with_namespace_column_for_node_ids(table=input_data)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


@pytest.mark.usefixtures("expected_edge_column_with_node_ids_ns_pair")
def test_add_namespace_column_pair(
    expected_edge_column_with_node_ids_ns_pair: DataFrame,
):
    table_schema = SCHEMA_MAPPING_TABLE + [
        analysis_utils.get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
        analysis_utils.get_namespace_column_name_for_column(COLUMN_TARGET_ID),
    ]
    input_data = pd.DataFrame(
        [
            (
                "MONDO:0000004",
                "MONDO:0000123",
                "equivalent_to",
                "MONDO",
                "MONDO",
                "MONDO",
            )
        ],
        columns=table_schema,
    )
    actual = analysis_utils.produce_table_with_namespace_column_pair(table=input_data)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected_edge_column_with_node_ids_ns_pair.values) is True


def test_add_namespace_column_pair_no_such_column():
    input_data = pd.DataFrame(["MONDO:0000004"], columns=[COLUMN_DEFAULT_ID])
    actual = analysis_utils.produce_table_with_namespace_column_pair(table=input_data)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, input_data.values) is True


@pytest.mark.usefixtures("expected_edge_column_with_node_ids_ns_pair")
def test_add_namespace_column_to_loaded_tables(
    expected_edge_column_with_node_ids_ns_pair: DataFrame,
):
    input_data = NamedTable(
        "FOO",
        pd.DataFrame(
            [("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO")],
            columns=SCHEMA_MAPPING_TABLE,
        ),
    )
    actual = analysis_utils.add_namespace_column_to_loaded_tables(tables=[input_data])
    assert isinstance(actual, list)
    for obj in actual:
        assert isinstance(obj, NamedTable)
    assert (
        np.array_equal(
            actual[0].dataframe.values,
            expected_edge_column_with_node_ids_ns_pair.values,
        )
        is True
    )


def test_produce_table_node_namespace_distribution():
    input_nodes = pd.DataFrame(["MONDO:0000001", "SNOMED:001", "MONDO:1234"], columns=[COLUMN_DEFAULT_ID])
    expected_1 = pd.DataFrame(
        [("MONDO", 2, "66.67%"), ("SNOMED", 1, "33.33%")],
        columns=SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE,
    )
    actual_1 = analysis_utils.produce_table_node_namespace_distribution(node_table=input_nodes)
    assert isinstance(actual_1, DataFrame)
    assert np.array_equal(actual_1.values, expected_1.values) is True

    actual_2 = analysis_utils.produce_table_node_namespace_distribution(
        node_table=pd.DataFrame([], columns=[COLUMN_DEFAULT_ID])
    )
    expected_2 = pd.DataFrame(
        [],
        columns=SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE,
    )
    assert isinstance(actual_2, DataFrame)
    assert np.array_equal(actual_2.values, expected_2.values) is True
