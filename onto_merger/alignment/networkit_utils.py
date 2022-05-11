"""Data class for Networkit graph."""

import sys
from dataclasses import dataclass
from typing import Dict, List

import networkit as nk
from networkit import Graph
from pandas import DataFrame

from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
)
from onto_merger.logger.log import get_logger

# from networkit import getPath


logger = get_logger(__name__)


class NetworkitGraph:
    graph: Graph
    node_id_to_index_map: Dict[str, int]
    node_index_to_id_map: Dict[int, str]
    root_nodes = List[str]
    search_heuristic: List[int]

    def __init__(self, edges: DataFrame):
        logger.info(f"Started initialising hierarchy graph from {len(edges):,d} edges...")
        src_ids = edges[COLUMN_SOURCE_ID].tolist()
        trg_ids = edges[COLUMN_TARGET_ID].tolist()
        node_ids = list(set(src_ids + trg_ids))
        self.root_nodes = [node_id for node_id in node_ids if node_id not in src_ids]
        logger.info(f"Producing node ID lookup maps..")
        self.node_id_to_index_map = NetworkitGraph._produce_node_id_to_index_map(node_ids=node_ids)
        self.node_index_to_id_map = NetworkitGraph._produce_node_index_to_id_map(
            node_id_to_index_map=self.node_id_to_index_map
        )
        logger.info(f"Adding edges..")
        self.graph = self._create_networkit_graph(edges=edges, node_id_to_index=self.node_id_to_index_map)
        self.search_heuristic = [0 for _ in range(self.graph.upperNodeIdBound())]
        logger.info(
            f"Hierarchy graph initialised with {self.graph.numberOfNodes():,d} nodes "
            + f"({len(self.root_nodes)} possible root(s)) and {self.graph.numberOfEdges():,d} edges"
        )

    def get_path_for_node(self, node_id: str) -> List[str]:
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
            if path:
                return [node_index] + path + [root_node_index]
        return []

    @staticmethod
    def _create_networkit_graph(edges: DataFrame, node_id_to_index: dict) -> Graph:
        """Produces a network x graph object from a hierarchy edge table.

        :param node_id_to_index:
        :param edges: The hierarchy edge table.
        :return: The network x graph.
        """
        graph = nk.Graph(len(node_id_to_index), weighted=False, directed=True)
        for _, row in edges.iterrows():
            graph.addEdge(node_id_to_index[row[COLUMN_SOURCE_ID]], node_id_to_index[row[COLUMN_TARGET_ID]])
        return graph

    @staticmethod
    def _produce_node_id_to_index_map(node_ids: List[str]) -> Dict[str, int]:
        return {node_id: node_ids.index(node_id) for node_id in node_ids}

    @staticmethod
    def _produce_node_index_to_id_map(node_id_to_index_map: Dict[str, int]) -> Dict[int, str]:
        return {node_index: node_id for node_id, node_index in node_id_to_index_map.items()}
