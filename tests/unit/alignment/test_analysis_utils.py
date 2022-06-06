"""Tests for the mapping_utils."""
import numpy as np
import pandas as pd
from pandas import DataFrame

from onto_merger.analyser import analysis_utils
from onto_merger.analyser.analysis_utils import get_namespace_column_name_for_column
from onto_merger.data.constants import COLUMN_DEFAULT_ID


def test_filter_nodes_for_namespace():
    input_nodes = pd.DataFrame(["MONDO:0000001", "SNOMED:001", "FOOBAR:1234"], columns=[COLUMN_DEFAULT_ID])
    expected = pd.DataFrame(
        [("MONDO:0000001", "MONDO")],
        columns=[
            COLUMN_DEFAULT_ID,
            get_namespace_column_name_for_column(COLUMN_DEFAULT_ID),
        ],
    )
    actual = analysis_utils.filter_nodes_for_namespace(nodes=input_nodes, namespace="MONDO")
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True
