import pandas as pd

from onto_merger.data.constants import SCHEMA_EDGE_SOURCE_TO_TARGET_IDS
from onto_merger.alignment.networkit_utils import NetworkitGraph


def test_get_hierarchy_edge_for_unmapped_node():
    background_knowledge_hierarchy_edges = pd.DataFrame(
        [("FOO:001", "FOO:002"), ("FOO:002", "FOO:003")],
        columns=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
    hierarchy_graph = NetworkitGraph(edges=background_knowledge_hierarchy_edges)

    # direct path
    expected_1 = [("FOO:001", "SNOMED:001")]
    actual_1 = hierarchy_graph.get_path_for_node(
        node_id="FOO:001",
    )
    print("expected_1 ", expected_1)
    print("actual_1 ", actual_1)
    assert isinstance(actual_1, list)
    assert actual_1 == expected_1

    # path with other unmapped nodes
    expected_2 = [("FOO:001", "FOO:002"), ("FOO:002", "SNOMED:001")]
    actual_2 = hierarchy_graph.get_path_for_node(
        node_id="FOO:001",
    )
    assert isinstance(actual_2, list)
    assert actual_2 == expected_2

    # no path: no edges for input node ID
    actual_3 = hierarchy_graph.get_path_for_node(
        node_id="FOO:00345",
    )
    assert isinstance(actual_3, list)
    assert actual_3 == []

    # no path: no edges with merged nodes for input node ID
    actual_4 = hierarchy_graph.get_path_for_node(
        node_id="FOO:001",
    )
    assert isinstance(actual_4, list)
    assert actual_4 == []
