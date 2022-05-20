import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

import analyser.analyser_tables
from onto_merger.alignment import hierarchy_utils
from onto_merger.alignment.networkit_utils import NetworkitGraph
from onto_merger.analyser import get_namespace_column_name_for_column
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    SCHEMA_HIERARCHY_EDGE_TABLE,
    SCHEMA_MERGE_TABLE,
)
from onto_merger.data.dataclasses import NamedTable


@pytest.fixture()
def example_hierarchy_edges():
    return pd.DataFrame(
        [
            ("MONDO:001", "MONDO:002", "sub", "MONDO"),
            ("MONDO:002", "MONDO:003", "sub", "MONDO"),
            ("FOO:001", "MONDO:002", "sub", "MONDO"),
        ],
        columns=SCHEMA_HIERARCHY_EDGE_TABLE,
    )


def test_produce_seed_ontology_hierarchy_table(example_hierarchy_edges):
    nodes = pd.DataFrame(["MONDO:001", "MONDO:002", "MONDO:003"], columns=[COLUMN_DEFAULT_ID])
    actual = hierarchy_utils._produce_table_seed_ontology_hierarchy(
        seed_ontology_name="MONDO",
        nodes=nodes,
        hierarchy_edges=example_hierarchy_edges,
    )
    expected = pd.DataFrame(
        [("MONDO:001", "MONDO:002", "sub", "MONDO"),
         ("MONDO:002", "MONDO:003", "sub", "MONDO")],
        columns=SCHEMA_HIERARCHY_EDGE_TABLE,
    )
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True

    actual2 = hierarchy_utils._produce_table_seed_ontology_hierarchy(
        seed_ontology_name="MO",
        nodes=nodes,
        hierarchy_edges=example_hierarchy_edges,
    )
    assert actual2 is None


def test_produce_table_nodes_only_connected():
    hierarchy_edges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
    merges = pd.DataFrame([("SNOMED:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    expected = pd.DataFrame(["FOO:001"], columns=[COLUMN_DEFAULT_ID])
    actual = analyser.analyser_tables.produce_named_table_nodes_only_connected(hierarchy_edges=hierarchy_edges, merges_aggregated=merges)
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_table_nodes_dangling():
    hierarchy_edges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
    merges = pd.DataFrame([("SNOMED:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    nodes = pd.DataFrame(["FOO:001", "SNOMED:001", "MONDO:002"], columns=[COLUMN_DEFAULT_ID])
    expected = pd.DataFrame(["MONDO:002"], columns=[COLUMN_DEFAULT_ID])
    actual = analyser.analyser_tables.produce_named_table_nodes_dangling(nodes=nodes, hierarchy_edges=hierarchy_edges, merges_aggregated=merges)
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_node_id_table_from_edge_table():
    edges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    expected = pd.DataFrame(["FOO:001", "SNOMED:001"], columns=[COLUMN_DEFAULT_ID])
    actual = analyser.analyser_tables.produce_table_node_ids_from_edge_table(edges=edges)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_produce_merged_node_id_list():
    merges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    expected = ["FOO:001"]
    actual = analyser.analyser_tables.produce_merged_node_id_list(merges_aggregated=merges)
    assert isinstance(actual, list)
    assert actual == expected


def test_filter_nodes_for_namespace():
    input_nodes = pd.DataFrame(["MONDO:0000001", "SNOMED:001", "FOOBAR:1234"], columns=[COLUMN_DEFAULT_ID])
    expected = pd.DataFrame(
        [("MONDO:0000001", "MONDO")],
        columns=[
            COLUMN_DEFAULT_ID,
            get_namespace_column_name_for_column(COLUMN_DEFAULT_ID),
        ],
    )
    actual = analyser.analyser_tables.filter_nodes_for_namespace(nodes=input_nodes, namespace="MONDO")
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_hierarchy_edge_for_unmapped_node():
    merge_map = {"FOO:003": "SNOMED:001"}
    background_knowledge_hierarchy_edges = pd.DataFrame(
        [("FOO:001", "FOO:002"), ("FOO:002", "FOO:003")],
        columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
    hierarchy_graph = NetworkitGraph(edges=background_knowledge_hierarchy_edges)

    # direct path
    expected_1 = [("FOO:001", "SNOMED:001")]
    actual_1 = hierarchy_utils._produce_hierarchy_path_for_unmapped_node(
        node_to_connect="FOO:001",
        unmapped_node_ids=["FOO:001"],
        merge_and_connectivity_map_for_ns=merge_map,
        hierarchy_graph_for_ns=hierarchy_graph
    )
    print("expected_1 ", expected_1)
    print("actual_1 ", actual_1)
    assert isinstance(actual_1, list)
    assert actual_1 == expected_1

    # path with other unmapped nodes
    expected_2 = [("FOO:001", "FOO:002"), ("FOO:002", "SNOMED:001")]
    actual_2 = hierarchy_utils._produce_hierarchy_path_for_unmapped_node(
        node_to_connect="FOO:001",
        unmapped_node_ids=["FOO:001", "FOO:002"],
        merge_and_connectivity_map_for_ns=merge_map,
        hierarchy_graph_for_ns=hierarchy_graph
    )
    assert isinstance(actual_2, list)
    assert actual_2 == expected_2

    # no path: no edges for input node ID
    actual_3 = hierarchy_utils._produce_hierarchy_path_for_unmapped_node(
        node_to_connect="FOO:00345",
        unmapped_node_ids=["FOO:001", "FOO:002"],
        merge_and_connectivity_map_for_ns=merge_map,
        hierarchy_graph_for_ns=hierarchy_graph
    )
    assert isinstance(actual_3, list)
    assert actual_3 == []

    # no path: no edges with merged nodes for input node ID
    actual_4 = hierarchy_utils._produce_hierarchy_path_for_unmapped_node(
        node_to_connect="FOO:001",
        unmapped_node_ids=["FOO:001", "FOO:002"],
        merge_and_connectivity_map_for_ns=merge_map,
        hierarchy_graph_for_ns=NetworkitGraph(
            edges=pd.DataFrame(
                [("FOO:001", "FOO:002")],
                columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
        )
    )
    assert isinstance(actual_4, list)
    assert actual_4 == []
