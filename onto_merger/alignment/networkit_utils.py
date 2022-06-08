"""Methods and data class for using the Networkit graph package."""

# mypy: ignore-errors

from typing import Dict, List

import networkit as nk
from networkit import Graph
from pandas import DataFrame
from tqdm import tqdm

from onto_merger.data.constants import COLUMN_SOURCE_ID, COLUMN_TARGET_ID
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


class NetworkitGraph:
    """Data class for using a Networkit graph."""

    graph: Graph
    node_id_to_index_map: Dict[str, int]
    node_index_to_id_map: Dict[int, str]
    root_nodes = List[str]
    search_heuristic: List[int]

    def __init__(self, edges: DataFrame):
        """Initialise the Graph class."""
        logger.info(f"Started initialising hierarchy graph from {len(edges):,d} edges...")
        src_ids = edges[COLUMN_SOURCE_ID].tolist()
        trg_ids = edges[COLUMN_TARGET_ID].tolist()
        node_ids = list(set(src_ids + trg_ids))
        self.root_nodes = [node_id for node_id in node_ids if node_id not in src_ids]
        logger.info("Producing node ID lookup maps..")
        self.node_id_to_index_map, self.node_index_to_id_map = NetworkitGraph._produce_node_id_maps(node_ids=node_ids)
        logger.info("Adding edges..")
        self.graph = self._create_networkit_graph(edges=edges, node_id_to_index=self.node_id_to_index_map)
        self.search_heuristic = [0 for _ in range(self.graph.upperNodeIdBound())]
        logger.info(
            f"Hierarchy graph initialised with {self.graph.numberOfNodes():,d} nodes "
            + f"({len(self.root_nodes)} possible root(s)) and {self.graph.numberOfEdges():,d} edges"
        )

    def get_path_for_node(self, node_id: str) -> List[str]:
        """Get the shortest path (to root) for a node.

        :param node_id: The node ID.
        :return: The shortest path.
        """
        if node_id not in self.node_id_to_index_map:
            return []
        return [
            self.node_index_to_id_map[node_index]
            for node_index in self._get_path_for_node_index(node_index=self.node_id_to_index_map[node_id])
        ]

    def _get_path_for_node_index(self, node_index: int) -> List[int]:
        for root_node_id in self.root_nodes:
            root_node_index = self.node_id_to_index_map[root_node_id]
            a_star = nk.distance.AStar(self.graph, self.search_heuristic, node_index, root_node_index)
            a_star.run()
            path = a_star.getPath()
            if path or (self.graph.hasEdge(node_index, root_node_index)):
                return [node_index] + path + [root_node_index]
        return []

    @staticmethod
    def _create_networkit_graph(edges: DataFrame, node_id_to_index: dict) -> Graph:
        """Produce a networkit graph object from a hierarchy edge table.

        :param node_id_to_index:
        :param edges: The hierarchy edge table.
        :return: The network x graph.
        """
        graph = nk.Graph(len(node_id_to_index), weighted=False, directed=True)
        with tqdm(total=len(edges), desc="Adding edges to Networkit graph") as progress_bar:
            for _, row in edges.iterrows():
                graph.addEdge(node_id_to_index[row[COLUMN_SOURCE_ID]], node_id_to_index[row[COLUMN_TARGET_ID]])
                progress_bar.update(1)
        return graph

    @staticmethod
    def _produce_node_id_maps(node_ids: List[str]) -> (Dict[str, int], Dict[int, str]):
        node_id_to_index_map = dict()
        node_index_to_id_map = dict()
        with tqdm(total=len(node_ids), desc="Producing node ID lookup maps") as progress_bar:
            for idx, node_id in enumerate(node_ids):
                node_id_to_index_map[node_id] = idx
                node_index_to_id_map[idx] = node_id
                progress_bar.update(1)
        return node_id_to_index_map, node_index_to_id_map
