"""Tests for the GE expectation helper methods."""
from typing import List

import pytest
from great_expectations.core import ExpectationConfiguration

from onto_merger.analyser.analysis_utils import get_namespace_column_name_for_column
from onto_merger.data.constants import (
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE_ID,
    COLUMN_SOURCE_TO_TARGET,
    COLUMN_TARGET_ID,
    RELATION_RDFS_SUBCLASS_OF,
    SCHEMA_HIERARCHY_EDGE_TABLE,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MAPPINGS,
    TABLE_MAPPINGS_DOMAIN,
    TABLE_MERGES,
    TABLE_NODES,
    TABLE_NODES_OBSOLETE,
    TABLE_NODES_UNMAPPED,
)
from onto_merger.data_testing import ge_expectation_helper
from tests.fixtures import alignment_config


def expectation_list_type_check(actual: List[ExpectationConfiguration]):
    assert isinstance(actual, List)
    assert len(actual) > 0
    for obj in actual:
        assert isinstance(obj, ExpectationConfiguration)


def test_produce_expectations_for_table(alignment_config):
    table_types_to_check = [
        TABLE_NODES,
        TABLE_NODES_OBSOLETE,
        TABLE_NODES_UNMAPPED,
        TABLE_EDGES_HIERARCHY,
        TABLE_EDGES_HIERARCHY_POST,
        TABLE_MAPPINGS,
        TABLE_MERGES,
    ]
    for table_type in table_types_to_check:
        expectation_list_type_check(
            actual=ge_expectation_helper.produce_expectations_for_table(
                table_name=table_type, alignment_config=alignment_config
            )
        )
    actual_empty = ge_expectation_helper.produce_expectations_for_table(
        table_name="FOO", alignment_config=alignment_config
    )
    assert actual_empty == []


def test_produce_node_table_expectations():
    actual = ge_expectation_helper.produce_node_table_expectations(table_name=TABLE_NODES)
    expectation_list_type_check(actual=actual)
    assert len(actual) == 12


def test_produce_edge_table_expectations(alignment_config):
    actual = ge_expectation_helper.produce_edge_table_expectations(
        table_name=TABLE_MERGES, alignment_config=alignment_config
    )
    expectation_list_type_check(actual=actual)
    assert len(actual) == 31


def test_get_column_set_for_edge_table():
    column_set = SCHEMA_HIERARCHY_EDGE_TABLE + [
        get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
        get_namespace_column_name_for_column(COLUMN_TARGET_ID),
        COLUMN_SOURCE_TO_TARGET
    ]

    # EDGE
    actual_1 = ge_expectation_helper.get_column_set_for_edge_table(table_name=TABLE_EDGES_HIERARCHY)
    assert isinstance(actual_1, List)
    assert set(actual_1) == set(column_set)

    # DOMAIN
    actual_2 = ge_expectation_helper.get_column_set_for_edge_table(table_name=TABLE_MAPPINGS_DOMAIN)
    assert isinstance(actual_2, List)
    assert set(actual_2) == set(SCHEMA_HIERARCHY_EDGE_TABLE)

    #
    actual_4 = ge_expectation_helper.get_column_set_for_edge_table(table_name="foo")
    assert isinstance(actual_4, List)
    assert actual_4 == []


def test_get_edge_types_for_edge_table(alignment_config):
    actual_1 = ge_expectation_helper.get_relation_types_for_edge_table(
        table_name=TABLE_EDGES_HIERARCHY, alignment_config=alignment_config
    )
    assert isinstance(actual_1, List)
    assert actual_1 == [RELATION_RDFS_SUBCLASS_OF]

    actual_2 = ge_expectation_helper.get_relation_types_for_edge_table(
        table_name=TABLE_MAPPINGS, alignment_config=alignment_config
    )
    assert isinstance(actual_2, List)
    assert len(actual_2) > 1
    for obj in actual_2:
        assert isinstance(obj, str)

    actual_3 = ge_expectation_helper.get_relation_types_for_edge_table(
        table_name="FOO", alignment_config=alignment_config
    )
    assert actual_3 == []


def test_produce_node_short_id_expectations():
    # not in node table
    actual_not_in_node_table = ge_expectation_helper.produce_node_short_id_expectations(
        column_name="foo", is_node_table=False
    )
    expectation_list_type_check(actual=actual_not_in_node_table)
    assert len(actual_not_in_node_table) == 5

    # in node table
    actual_in_node_table = ge_expectation_helper.produce_node_short_id_expectations(
        column_name="foo", is_node_table=True
    )
    expectation_list_type_check(actual=actual_in_node_table)
    assert len(actual_not_in_node_table) == 5


def test_produce_edge_relation_expectations():
    actual = ge_expectation_helper.produce_edge_relation_expectations(column_name="foo", edge_types=["fizz", "bang"])
    expectation_list_type_check(actual=actual)
    assert len(actual) == 5


def test_produce_expectation_config_column_type_string():
    expected = {
        "kwargs": {"column": "foo", "mostly": 1.0, "type_": "object"},
        "meta": {},
        "expectation_type": "expect_column_values_to_be_of_type",
    }
    actual = ge_expectation_helper.produce_expectation_config_column_type_string(column_name="foo")
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected


def test_produce_expectation_config_column_values_to_not_be_null():
    expected = {
        "kwargs": {"column": "foo", "mostly": 1.0},
        "meta": {},
        "expectation_type": "expect_column_values_to_not_be_null",
    }
    actual = ge_expectation_helper.produce_expectation_config_column_values_to_not_be_null(column_name="foo")
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected


def test_produce_expectation_config_column_values_to_be_unique():
    expected = {
        "kwargs": {"column": "foo", "mostly": 1.0},
        "meta": {},
        "expectation_type": "expect_column_values_to_be_unique",
    }
    actual = ge_expectation_helper.produce_expectation_config_column_values_to_be_unique(column_name="foo")
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected


def test_produce_expectation_config_column_value_lengths_to_be_between():
    expected = {
        "kwargs": {"column": "foo", "mostly": 1.0, "min_value": 1, "max_value": 10},
        "meta": {},
        "expectation_type": "expect_column_value_lengths_to_be_between",
    }
    actual = ge_expectation_helper.produce_expectation_config_column_value_lengths_to_be_between(
        column_name="foo", min_value=1, max_value=10
    )
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected


def test_produce_expectation_config_column_values_to_match_regex():
    expected = {
        "kwargs": {"column": "foo", "regex": "[abc]", "mostly": 1.0},
        "meta": {},
        "expectation_type": "expect_column_values_to_match_regex",
    }
    actual = ge_expectation_helper.produce_expectation_config_column_values_to_match_regex(
        column_name="foo", regex="[abc]"
    )
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected


def test_produce_expectation_config_expect_column_to_exist():
    expected = {
        "kwargs": {"column": "foo"},
        "meta": {},
        "expectation_type": "expect_column_to_exist",
    }
    actual = ge_expectation_helper.produce_expectation_config_expect_column_to_exist(column_name="foo")
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected


def test_produce_expectation_config_expect_column_values_to_be_in_set():
    expected = {
        "kwargs": {"column": "foo", "value_set": ["a", "b", "c"], "mostly": 1.0},
        "meta": {},
        "expectation_type": "expect_column_values_to_be_in_set",
    }
    actual = ge_expectation_helper.produce_expectation_config_expect_column_values_to_be_in_set(
        column_name="foo", value_set=["a", "b", "c"]
    )
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected


def test_produce_expectation_config_expect_table_columns_to_match_set():
    expected = {
        "kwargs": {"column_set": ["foo", "bar"], "exact_match": True},
        "meta": {},
        "expectation_type": "expect_table_columns_to_match_set",
    }
    actual = ge_expectation_helper.produce_expectation_config_expect_table_columns_to_match_set(
        column_set=["foo", "bar"]
    )
    assert isinstance(actual, ExpectationConfiguration)
    assert actual.to_json_dict() == expected
