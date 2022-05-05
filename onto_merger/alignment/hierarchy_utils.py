import itertools
from typing import List, Optional, Tuple

import pandas as pd
from networkx import Graph
from pandas import DataFrame

from onto_merger.alignment import networkx_utils, mapping_utils
from onto_merger.analyser.analysis_util import (
    get_namespace_column_name_for_column,
    produce_table_with_namespace_column_for_node_ids
)
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_NODES,
    TABLE_NODES_CONNECTED_ONLY,
    TABLE_NODES_DANGLING,
    SCHEMA_MERGE_TABLE, TABLE_MERGES_AGGREGATED, TABLE_NODES_UNMAPPED)
from onto_merger.data.dataclasses import AlignmentConfig, DataRepository, NamedTable
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def connect_nodes(alignment_config: AlignmentConfig,
                  source_alignment_order: List[str],
                  data_repo: DataRepository) -> NamedTable:
    """Runs the connectivity process to establish a hierarchy between the domain nodes.

    :param source_alignment_order: The source alignment order.
    :param alignment_config: The alignment process configuration dataclass.
    :param data_repo: The data repository containing the input data.
    :return: The produced hierarchy edge table.
    """
    # (1) get the seed hierarchy as main scaffolding
    seed_hierarchy_df = produce_table_seed_ontology_hierarchy(
        seed_ontology_name=alignment_config.base_config.seed_ontology_name,
        nodes=data_repo.get(TABLE_NODES).dataframe,
        hierarchy_edges=data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
    )

    # (2) connect unmapped nodes to the seed hierarchy
    unmapped_node_hierarchy_df = produce_hierarchy_edges_for_unmapped_nodes(
        unmapped_nodes=data_repo.get(TABLE_NODES_UNMAPPED).dataframe,
        merges=data_repo.get(TABLE_MERGES_AGGREGATED).dataframe,
        source_alignment_order=source_alignment_order,
        hierarchy_edges=data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
    )

    # (3) return the merged hierarchy
    return NamedTable(
        name=TABLE_EDGES_HIERARCHY_POST,
        dataframe=pd.concat(
            [seed_hierarchy_df, unmapped_node_hierarchy_df]).drop_duplicates(
            keep="first"),
    )


def produce_table_seed_ontology_hierarchy(
        seed_ontology_name: str, nodes: DataFrame, hierarchy_edges: DataFrame
) -> Optional[DataFrame]:
    """Produces the hierarchy edge table for the seed ontology nodes.

    :param seed_ontology_name: The name of the seed ontology.
    :param nodes: The full set of domain nodes (including seed nodes).
    :param hierarchy_edges: All full set of hierarchy edges (including the seed edges).
    :return: The produced hierarchy edge table.
    """
    # get hierarchy of the seed ontology, filter out any non seed nodes
    # (nodes only have the type 'correct' IDs, whereas the edges may contain other ones)
    seed_node_list = list(
        filter_nodes_for_namespace(
            nodes=nodes,
            namespace=seed_ontology_name,
        )[COLUMN_DEFAULT_ID]
    )
    seed_hierarchy_table = hierarchy_edges.query(
        f"{COLUMN_SOURCE_ID} in @node_ids & {COLUMN_TARGET_ID} in @node_ids",
        local_dict={"node_ids": seed_node_list},
        inplace=False,
    )

    # check if the hierarchy is still one network (DAG)
    graph = networkx_utils.create_networkx_graph(edges=seed_hierarchy_table)
    if networkx_utils.is_single_subgraph(graph=graph) is False:
        logger.error("Error hierarchy is not a single DAG")
        return None
    else:
        return seed_hierarchy_table


def produce_table_nodes_only_connected(hierarchy_edges: DataFrame,
                                       merges: DataFrame) -> NamedTable:
    """Produces a table containing nodes that are not merged but connected (i.e. are
    in an edge hierarchy).

    :param hierarchy_edges: The domain node hierarchy.
    :param merges: The domain node merges.
    :return: The table of nodes connected that are not merged but connected.
    """
    nodes_connected = produce_node_id_table_from_edge_table(edges=hierarchy_edges)
    merged_node_ids = produce_merged_node_id_list(merges=merges)
    nodes_only_connected = nodes_connected.query(
        f"{COLUMN_DEFAULT_ID} not in @node_ids",
        local_dict={"node_ids": merged_node_ids},
        inplace=False,
    )
    return NamedTable(TABLE_NODES_CONNECTED_ONLY, nodes_only_connected)


def produce_table_nodes_dangling(nodes: DataFrame, hierarchy_edges: DataFrame,
                                 merges: DataFrame) -> NamedTable:
    """Produces a table containing nodes that are not merged or connected.

    :param nodes: The set of domain nodes (including seed nodes).
    :param hierarchy_edges: The domain node hierarchy.
    :param merges: The domain node merges.
    :return: The table of nodes connected that are not merged but connected.
    """
    connected_nodes_ids = (
        produce_table_nodes_only_connected(
            hierarchy_edges=hierarchy_edges,
            merges=merges
        ).dataframe[COLUMN_DEFAULT_ID].tolist()
    )
    connected_or_merged_node_ids = produce_merged_node_id_list(
        merges=merges) + connected_nodes_ids
    nodes_dangling = nodes.query(
        f"{COLUMN_DEFAULT_ID} not in @node_ids",
        local_dict={"node_ids": connected_or_merged_node_ids},
        inplace=False,
    )
    return NamedTable(TABLE_NODES_DANGLING, nodes_dangling)


def produce_node_id_table_from_edge_table(edges: DataFrame):
    """Produces a node ID table (i.e. a table containing node IDs only) from a
    given edge set by aggregating the source and target node IDs.

    :param edges: The edge table.
    :return: The node table with unique node IDs.
    """
    nodes_source = \
        (edges[[COLUMN_SOURCE_ID]]).rename(
            columns={COLUMN_SOURCE_ID: COLUMN_DEFAULT_ID},
            inplace=False)[
            [COLUMN_DEFAULT_ID]
        ]
    nodes_target = \
        (edges[[COLUMN_TARGET_ID]]).rename(
            columns={COLUMN_TARGET_ID: COLUMN_DEFAULT_ID},
            inplace=False)[
            [COLUMN_DEFAULT_ID]
        ]
    return pd.concat([nodes_source, nodes_target]).drop_duplicates(keep="first")


def produce_merged_node_id_list(merges: DataFrame) -> List[str]:
    """Produces a list of node IDs that are merged (the source node ID).

    :param merges: The table of merges.
    :return: The merged node IDs as a list.
    """
    return merges[COLUMN_SOURCE_ID].tolist()


def filter_nodes_for_namespace(nodes: DataFrame, namespace: str) -> DataFrame:
    """Filters a given node dataframe for a namespace.

    :param nodes: The node table to be filtered.
    :param namespace: The ontology ID.
    :return: The node dataframe where all nodes belong to the same ontology (namespace).
    """
    nodes_copy = nodes.copy()
    default_id_ns = get_namespace_column_name_for_column(COLUMN_DEFAULT_ID)
    if default_id_ns not in list(nodes_copy):
        nodes_copy = produce_table_with_namespace_column_for_node_ids(table=nodes_copy)
    nodes_for_namespace = nodes_copy.query(
        f'{default_id_ns} == "{namespace}"', inplace=False
    )
    logger.info(f"Found {len(nodes_for_namespace):,d} nodes for namespace "
                + f"{namespace} from total {len(nodes):,d} nodes.")
    return nodes_for_namespace


def produce_hierarchy_edges_for_unmapped_nodes(
        unmapped_nodes: DataFrame,
        merges: DataFrame,
        source_alignment_order: List[str],
        hierarchy_edges: DataFrame) -> DataFrame:
    # contains all merges; iteratively extended with connected nodes (where the node will "merge" to itself)
    # this provides a single data structure to identify terminus nodes in hierarchy paths, i.e. where
    # the path can be terminated while establishing connectivity to the main hierarchy
    merge_and_connectivity_map = _produce_merge_map(merges=merges)
    connectivity_order = source_alignment_order[1:]
    edges_for_all_nodes = []

    for node_namespace in connectivity_order:
        # produce the hierarchy edges for the namespace node set
        edges_for_namespace_nodes, merge_and_connectivity_map_for_ns = \
            _produce_hierarchy_edges_for_unmapped_nodes_of_namespace(
                node_namespace=node_namespace,
                unmapped_nodes=unmapped_nodes,
                hierarchy_edges=hierarchy_edges,
                merge_and_connectivity_map=merge_and_connectivity_map
            )
        if edges_for_namespace_nodes:
            # update result and processing data structures
            edges_for_all_nodes.extend(edges_for_namespace_nodes)
            merge_and_connectivity_map = merge_and_connectivity_map_for_ns

    # edges
    connected_nodes = [node_id for node_id in unmapped_nodes[COLUMN_DEFAULT_ID].tolist()
                       if node_id in merge_and_connectivity_map]
    new_hierarchy_edges = pd.DataFrame(edges_for_all_nodes, columns=SCHEMA_MERGE_TABLE)
    logger.info(f"Out of {len(unmapped_nodes):,d} unmapped nodes, "
                + f"{len(connected_nodes):,d} are now connected, "
                + f"via {len(new_hierarchy_edges):,d} hierarchy edges.")
    return new_hierarchy_edges


def _produce_hierarchy_edges_for_unmapped_nodes_of_namespace(
        node_namespace: str,
        unmapped_nodes: DataFrame,
        hierarchy_edges: DataFrame,
        merge_and_connectivity_map: dict
) -> [List[Tuple[str, str]], dict]:
    merge_and_connectivity_map_for_ns = merge_and_connectivity_map.copy()

    # get the unmapped node IDs for the namespace
    unmapped_node_ids_for_namespace = filter_nodes_for_namespace(
        nodes=unmapped_nodes, namespace=node_namespace
    )[COLUMN_DEFAULT_ID].tolist()
    logger.info(f"Starting to establish connectivity for {node_namespace} "
                + f"({len(unmapped_node_ids_for_namespace):,d} unmapped)")
    if not unmapped_node_ids_for_namespace:
        return []

    # create the hierarchy graph for the namespace
    hierarchy_graph_for_ns = networkx_utils.create_networkx_graph(
        edges=mapping_utils.get_mappings_for_namespace(
            namespace=node_namespace,
            edges=hierarchy_edges
        )
    )

    # connect
    edges_for_namespace_nodes = []
    for node_to_connect in unmapped_node_ids_for_namespace:
        if unmapped_node_ids_for_namespace.index(node_to_connect) % 100 == 0:
            logger.info(f"{node_namespace} | {unmapped_node_ids_for_namespace.index(node_to_connect):,d} "
                        + f"of {len(unmapped_node_ids_for_namespace):,d}")
        if node_to_connect not in merge_and_connectivity_map:
            edges_for_node = produce_hierarchy_path_for_unmapped_node(
                node_to_connect=node_to_connect,
                unmapped_node_ids=unmapped_node_ids_for_namespace,
                merge_and_connectivity_map_for_ns=merge_and_connectivity_map_for_ns,
                hierarchy_graph_for_ns=hierarchy_graph_for_ns
            )
            if edges_for_node:
                # update result and processing data structures
                edges_for_namespace_nodes.extend(edges_for_node)
                merge_and_connectivity_map_for_ns.update(
                    {item: item for item in list(itertools.chain(*edges_for_node))[0:-1]}
                )

    # results
    connected_nodes = [node_id for node_id in unmapped_node_ids_for_namespace
                       if node_id in merge_and_connectivity_map_for_ns]
    logger.info(f"Out of {len(unmapped_node_ids_for_namespace):,d} unmapped nodes of '{node_namespace}', "
                + f"{len(connected_nodes):,d} are now connected, "
                + f"via {len(edges_for_namespace_nodes):,d} hierarchy edges.")

    return edges_for_namespace_nodes, merge_and_connectivity_map_for_ns


def produce_hierarchy_path_for_unmapped_node(
        node_to_connect: str,
        unmapped_node_ids: List[str],
        merge_and_connectivity_map_for_ns: dict,
        hierarchy_graph_for_ns: Graph) -> List[Tuple[str, str]]:
    # get paths paths with merged nodes
    paths_terminating_with_a_merged_node = _get_hierarchy_paths_with_merged_nodes(
        shortest_paths=networkx_utils.get_shortest_paths_for_node(
            node_id=node_to_connect,
            graph=hierarchy_graph_for_ns
        ),
        merge_map=merge_and_connectivity_map_for_ns
    )
    if not paths_terminating_with_a_merged_node:
        return []

    # find the shortest path with a merged node
    shortest_path = _find_shortest_path_with_a_merged_node(
        paths_terminating_with_a_merged_node=paths_terminating_with_a_merged_node
    )

    # prune path between the unmapped and the merged node: remove all nodes that are not
    # also unmapped
    pruned_path = _prune_hierarchy_path(
        shortest_path=shortest_path,
        unmapped_node_ids=unmapped_node_ids,
        merge_map=merge_and_connectivity_map_for_ns
    )

    # convert the path into a hierarchy edge tuple list
    edges = _convert_hierarchy_path_into_tuple_list(pruned_path=pruned_path)

    return edges


def _produce_merge_map(merges: DataFrame) -> dict:
    return {
        row[COLUMN_SOURCE_ID]: row[COLUMN_TARGET_ID]
        for _, row in merges.iterrows()
    }


def _get_hierarchy_paths_with_merged_nodes(shortest_paths, merge_map: dict) -> dict:
    if not shortest_paths:
        return {}
    merged_node_ids_in_paths = [node_id
                                for node_id in shortest_paths.keys()
                                if node_id in merge_map.keys()]
    path_dict_terminating_with_a_merged_node = {
        key: value
        for key, value in shortest_paths.items()
        if key in merged_node_ids_in_paths
    }
    return path_dict_terminating_with_a_merged_node


def _find_shortest_path_with_a_merged_node(paths_terminating_with_a_merged_node: dict) -> List[str]:
    path_list_terminating_with_a_merged_node = \
        list(paths_terminating_with_a_merged_node.values())
    path_list_terminating_with_a_merged_node.sort(key=len)
    shortest_path = path_list_terminating_with_a_merged_node[0]
    return shortest_path


def _prune_hierarchy_path(shortest_path: List[str], unmapped_node_ids: List[str], merge_map: dict) -> List[str]:
    return [node_id
            for node_id in shortest_path
            if node_id in unmapped_node_ids] + [merge_map[shortest_path[-1]]]


def _convert_hierarchy_path_into_tuple_list(pruned_path: List[str]) -> List[Tuple[str, str]]:
    return [
        (pruned_path[source_index], pruned_path[source_index + 1])
        for source_index in range(0, (len(pruned_path) - 1))
    ]
