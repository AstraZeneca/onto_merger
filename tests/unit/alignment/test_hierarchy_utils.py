from typing import Tuple

import networkit as nk
import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from onto_merger.alignment import hierarchy_utils, networkit_utils, networkx_utils
from onto_merger.alignment.hierarchy_utils import produce_node_id_table_from_edge_table
from onto_merger.alignment.networkit_utils import NetworkitGraph
from onto_merger.analyser import get_namespace_column_name_for_column
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    SCHEMA_MERGE_TABLE,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_NODES,
    TABLE_NODES_UNMAPPED, TABLE_MERGES_AGGREGATED)
from onto_merger.data.dataclasses import DataRepository, NamedTable
from tests.fixtures import alignment_config


@pytest.fixture()
def example_hierarchy_edges():
    return pd.DataFrame(
        [
            ("MONDO:001", "MONDO:002"),
            ("MONDO:002", "MONDO:003"),
            ("FOO:001", "MONDO:002"),
        ],
        columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    )


# def test_connect_nodes(alignment_config, example_hierarchy_edges):
#     data_repo = DataRepository()
#     data_repo.update(
#         tables=[
#             NamedTable(TABLE_EDGES_HIERARCHY, example_hierarchy_edges),
#             NamedTable(
#                 TABLE_NODES,
#                 pd.DataFrame(["MONDO:001", "MONDO:002", "MONDO:003"], columns=[COLUMN_DEFAULT_ID]),
#             ),
#             NamedTable(
#                 TABLE_NODES_UNMAPPED,
#                 pd.DataFrame(["FOO:002"], columns=[COLUMN_DEFAULT_ID]),
#             ),
#             NamedTable(
#                 TABLE_MERGES_AGGREGATED,
#                 pd.DataFrame([("FOO:003", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
#             )
#         ]
#     )
#     expected = pd.DataFrame(
#         [("MONDO:001", "MONDO:002"), ("MONDO:002", "MONDO:003")],
#         columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
#     )
#     actual = hierarchy_utils.connect_nodes(
#         alignment_config=alignment_config,
#         data_repo=data_repo,
#         source_alignment_order=["MONDO", "FOO"]
#     )
#     assert isinstance(actual, NamedTable)
#     assert isinstance(actual.dataframe, DataFrame)
#     assert actual.name == TABLE_EDGES_HIERARCHY_POST
#     # assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_seed_ontology_hierarchy_table(example_hierarchy_edges):
    nodes = pd.DataFrame(["MONDO:001", "MONDO:002", "MONDO:003"], columns=[COLUMN_DEFAULT_ID])
    actual = hierarchy_utils.produce_table_seed_ontology_hierarchy(
        seed_ontology_name="MONDO",
        nodes=nodes,
        hierarchy_edges=example_hierarchy_edges,
    )
    expected = pd.DataFrame(
        [("MONDO:001", "MONDO:002"), ("MONDO:002", "MONDO:003")],
        columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    )
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True

    actual2 = hierarchy_utils.produce_table_seed_ontology_hierarchy(
        seed_ontology_name="MO",
        nodes=nodes,
        hierarchy_edges=example_hierarchy_edges,
    )
    assert actual2 is None


def test_produce_table_nodes_only_connected():
    hierarchy_edges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
    merges = pd.DataFrame([("SNOMED:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    expected = pd.DataFrame(["FOO:001"], columns=[COLUMN_DEFAULT_ID])
    actual = hierarchy_utils.produce_table_nodes_only_connected(hierarchy_edges=hierarchy_edges, merges=merges)
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_table_nodes_dangling():
    hierarchy_edges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
    merges = pd.DataFrame([("SNOMED:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    nodes = pd.DataFrame(["FOO:001", "SNOMED:001", "MONDO:002"], columns=[COLUMN_DEFAULT_ID])
    expected = pd.DataFrame(["MONDO:002"], columns=[COLUMN_DEFAULT_ID])
    actual = hierarchy_utils.produce_table_nodes_dangling(nodes=nodes, hierarchy_edges=hierarchy_edges, merges=merges)
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe.values, expected.values) is True


def test_produce_node_id_table_from_edge_table():
    edges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    expected = pd.DataFrame(["FOO:001", "SNOMED:001"], columns=[COLUMN_DEFAULT_ID])
    actual = hierarchy_utils.produce_node_id_table_from_edge_table(edges=edges)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_produce_merged_node_id_list():
    merges = pd.DataFrame([("FOO:001", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
    expected = ["FOO:001"]
    actual = hierarchy_utils.produce_merged_node_id_list(merges=merges)
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
    actual = hierarchy_utils.filter_nodes_for_namespace(nodes=input_nodes, namespace="MONDO")
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
    actual_1 = hierarchy_utils.produce_hierarchy_path_for_unmapped_node(
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
    actual_2 = hierarchy_utils.produce_hierarchy_path_for_unmapped_node(
        node_to_connect="FOO:001",
        unmapped_node_ids=["FOO:001", "FOO:002"],
        merge_and_connectivity_map_for_ns=merge_map,
        hierarchy_graph_for_ns=hierarchy_graph
    )
    assert isinstance(actual_2, list)
    assert actual_2 == expected_2

    # no path: no edges for input node ID
    actual_3 = hierarchy_utils.produce_hierarchy_path_for_unmapped_node(
        node_to_connect="FOO:00345",
        unmapped_node_ids=["FOO:001", "FOO:002"],
        merge_and_connectivity_map_for_ns=merge_map,
        hierarchy_graph_for_ns=hierarchy_graph
    )
    assert isinstance(actual_3, list)
    assert actual_3 == []

    # no path: no edges with merged nodes for input node ID
    actual_4 = hierarchy_utils.produce_hierarchy_path_for_unmapped_node(
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


def test_nk():

    print("FOO")

    background_knowledge_hierarchy_edges = pd.DataFrame(
        [("FOO:004", "ABC:005"), ("FOO:001", "FOO:002"), ("FOO:003", "FOO:004"), ("FOO:002", "FOO:003")],
        columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    )

    nk_graph = NetworkitGraph(edges=background_knowledge_hierarchy_edges)

    print("path for node FOO:001", nk_graph.get_path_for_node(node_id="FOO:001"))
    print("path for node FOO:003", nk_graph.get_path_for_node(node_id="FOO:003"))
    print("path for node ABC:005", nk_graph.get_path_for_node(node_id="ABC:005"))


# def test_produce_hierarchy_edges_for_unmapped_nodes():
#     merges = pd.DataFrame([("FOO:003", "SNOMED:001")], columns=SCHEMA_MERGE_TABLE)
#     background_knowledge_hierarchy_edges = pd.DataFrame(
#         [("FOO:001", "FOO:002"), ("FOO:002", "FOO:003")],
#         columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
#     actual = hierarchy_utils.produce_hierarchy_edges_for_unmapped_nodes(
#         unmapped_nodes=["FOO:001", "FOO:002", "FIZZ:123"],
#         merges=merges,
#         seed_ontology_name="MONDO",
#         hierarchy_edges=background_knowledge_hierarchy_edges
#     )
#
#     assert isinstance(actual, DataFrame)
