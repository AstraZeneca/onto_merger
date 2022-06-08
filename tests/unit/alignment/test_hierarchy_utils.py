import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from onto_merger.alignment import hierarchy_utils
from onto_merger.data.constants import COLUMN_DEFAULT_ID, SCHEMA_HIERARCHY_EDGE_TABLE
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


def test_produce_table_nodes_dangling():
    nodes = pd.DataFrame(["FOO:001", "SNOMED:001", "MONDO:002"], columns=[COLUMN_DEFAULT_ID])
    nodes_connected = pd.DataFrame(["FOO:001", "SNOMED:001"], columns=[COLUMN_DEFAULT_ID])
    expected = pd.DataFrame(["MONDO:002"], columns=[COLUMN_DEFAULT_ID])
    actual = hierarchy_utils.produce_named_table_nodes_dangling(nodes_all=nodes,
                                                                nodes_connected=nodes_connected)
    assert isinstance(actual, NamedTable)
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe.values, expected.values) is True
