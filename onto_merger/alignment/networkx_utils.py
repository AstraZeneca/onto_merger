"""Helper methods for using the Networkx graph package."""

from typing import List

import networkx as nx
from networkx import Graph, connected_components
from pandas import DataFrame

from onto_merger.data.constants import COLUMN_SOURCE_ID, COLUMN_TARGET_ID
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def create_networkx_graph(edges: DataFrame) -> Graph:
    """Produce a network x graph object from a hierarchy edge table.

    :param edges: The hierarchy edge table.
    :return: The network x graph.
    """
    graph: Graph = nx.from_pandas_edgelist(df=edges, source=COLUMN_SOURCE_ID, target=COLUMN_TARGET_ID)
    return graph


def is_single_subgraph(graph: Graph) -> bool:
    """Determine if there is only one sub-graph in a given network x graph.

    :param graph: The graph to check for sub-graph count.
    :return: True if there is only one sub-graph, otherwise False.
    """
    sub_graphs: List[Graph] = list(graph.subgraph(c) for c in connected_components(graph))
    if len(sub_graphs) == 1:
        return True
    else:
        [logger.debug(f"\tsub-graph: {sub} | nodes: {sub.nodes}") for sub in sub_graphs]
    return False
