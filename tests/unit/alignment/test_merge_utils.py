"""Tests for the merge utils methods."""

import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from onto_merger.alignment import merge_utils
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    SCHEMA_MERGE_TABLE,
    SCHEMA_MERGE_TABLE_WITH_META_DATA,
    TABLE_MERGES_WITH_META_DATA,
    TABLE_NODES_MERGED,
)
from onto_merger.data.dataclasses import NamedTable


@pytest.fixture()
def example_merges() -> DataFrame:
    return pd.DataFrame(
        [("A:1", "B:1"), ("B:1", "C:1"), ("D:2", "E:2")],
        columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    )


def test_aggregate_merges(example_merges):
    table_schema = [COLUMN_SOURCE_ID, COLUMN_TARGET_ID]

    alignment_priority_order = ["C", "E", "B", "D", "A"]

    actual = merge_utils._produce_named_table_aggregated_merges(
        merges=example_merges, alignment_priority_order=alignment_priority_order
    ).dataframe

    output_data = [("A:1", "C:1"), ("B:1", "C:1"), ("D:2", "E:2")]
    expected = pd.DataFrame(output_data, columns=table_schema)

    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_canonical_node_for_merge_cluster():
    # canonical exits
    canonical_node = "C:1"
    input_cluster = ["A:1", "B:1", canonical_node]
    input_alignment_priority_order = ["C", "B", "A"]
    actual = merge_utils._get_canonical_node_for_merge_cluster(
        merge_cluster=input_cluster,
        alignment_priority_order=input_alignment_priority_order,
    )
    assert isinstance(actual, str)
    assert actual == canonical_node

    # canonical cannot be found
    actual_no_canonical = merge_utils._get_canonical_node_for_merge_cluster(
        merge_cluster=input_cluster,
        alignment_priority_order=["X"],
    )
    assert actual_no_canonical is None


def test_produce_named_table_merged_nodes(example_merges):
    expected = pd.DataFrame(["A:1", "B:1", "D:2"], columns=[[COLUMN_DEFAULT_ID]])
    actual = merge_utils._produce_named_table_merged_nodes(merges_aggregated=example_merges)
    assert isinstance(actual, NamedTable)
    assert actual.name == TABLE_NODES_MERGED
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_named_table_merges_with_alignment_meta_data(example_merges):
    step = 0
    source_id = "FOO"
    mapping_type = "eqv"
    expected = pd.DataFrame(
        [
            ("A:1", "B:1", step, source_id, mapping_type),
            ("B:1", "C:1", step, source_id, mapping_type),
            ("D:2", "E:2", step, source_id, mapping_type),
        ],
        columns=SCHEMA_MERGE_TABLE_WITH_META_DATA,
    )
    actual = merge_utils.produce_named_table_merges_with_alignment_meta_data(
        merges=example_merges,
        source_id=source_id,
        step_counter=step,
        mapping_type=mapping_type,
    )
    assert isinstance(actual, NamedTable)
    assert actual.name == TABLE_MERGES_WITH_META_DATA
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_table_unmapped_nodes():
    # input
    input_merges = pd.DataFrame(
        [("FOOBAR:1234", "MONDO:0000123"), ("FIZZBANG:0000001", "MONDO:0000456")],
        columns=SCHEMA_MERGE_TABLE,
    )
    input_nodes = pd.DataFrame(["FIZZBANG:0000001", "SNOMED:001", "FOOBAR:1234"], columns=[COLUMN_DEFAULT_ID])

    # run
    actual = merge_utils.produce_table_unmapped_nodes(nodes=input_nodes, merges=input_merges)

    # expected
    expected = pd.DataFrame(["SNOMED:001"], columns=[COLUMN_DEFAULT_ID])

    # evaluate
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True
