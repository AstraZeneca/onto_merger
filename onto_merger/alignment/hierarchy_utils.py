"""Methods to produce node hierarchy and analyse node connectivity status."""

import dataclasses
import itertools
import sys
from typing import List, Optional, Tuple

import pandas as pd
from pandas import DataFrame

from onto_merger.alignment import mapping_utils, networkx_utils
from onto_merger.alignment.networkit_utils import NetworkitGraph
from onto_merger.analyser.analysis_util import (
    get_namespace_column_name_for_column,
    produce_table_with_namespace_column_for_node_ids,
)
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE,
    SCHEMA_MERGE_TABLE,
    TABLE_CONNECTIVITY_STEPS_REPORT,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MERGES_AGGREGATED,
    TABLE_NODES,
    TABLE_NODES_CONNECTED_ONLY,
    TABLE_NODES_DANGLING,
    TABLE_NODES_UNMAPPED,
)
from onto_merger.data.dataclasses import (
    AlignmentConfig,
    ConnectivityStep,
    DataRepository,
    NamedTable,
)
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def connect_nodes(
    alignment_config: AlignmentConfig, source_alignment_order: List[str], data_repo: DataRepository
) -> List[NamedTable]:
    """Run the connectivity process to establish a hierarchy between the domain nodes.

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
    unmapped_node_hierarchy_df, connectivity_steps = _produce_hierarchy_edges_for_unmapped_nodes(
        unmapped_nodes=data_repo.get(TABLE_NODES_UNMAPPED).dataframe,
        merges=data_repo.get(TABLE_MERGES_AGGREGATED).dataframe,
        source_alignment_order=source_alignment_order,
        hierarchy_edges=data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
    )

    # (3) return the merged hierarchy
    return [
        NamedTable(
            name=TABLE_EDGES_HIERARCHY_POST,
            dataframe=pd.concat([seed_hierarchy_df, unmapped_node_hierarchy_df]).drop_duplicates(keep="first"),
        ),
        _convert_connectivity_steps_to_named_table(steps=connectivity_steps),
    ]


def produce_table_seed_ontology_hierarchy(
    seed_ontology_name: str, nodes: DataFrame, hierarchy_edges: DataFrame
) -> Optional[DataFrame]:
    """Produce the hierarchy edge table for the seed ontology nodes.

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


def produce_table_nodes_only_connected(hierarchy_edges: DataFrame, merges: DataFrame) -> NamedTable:
    """Produce a table containing nodes that are not merged but connected (i.e. are in an edge hierarchy).

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


def produce_table_nodes_dangling(nodes: DataFrame, hierarchy_edges: DataFrame, merges: DataFrame) -> NamedTable:
    """Produce a table containing nodes that are not merged or connected.

    :param nodes: The set of domain nodes (including seed nodes).
    :param hierarchy_edges: The domain node hierarchy.
    :param merges: The domain node merges.
    :return: The table of nodes connected that are not merged but connected.
    """
    connected_nodes_ids = (
        produce_table_nodes_only_connected(hierarchy_edges=hierarchy_edges, merges=merges)
        .dataframe[COLUMN_DEFAULT_ID]
        .tolist()
    )
    connected_or_merged_node_ids = produce_merged_node_id_list(merges=merges) + connected_nodes_ids
    nodes_dangling = nodes.query(
        f"{COLUMN_DEFAULT_ID} not in @node_ids",
        local_dict={"node_ids": connected_or_merged_node_ids},
        inplace=False,
    )
    return NamedTable(TABLE_NODES_DANGLING, nodes_dangling)


def produce_node_id_table_from_edge_table(edges: DataFrame) -> DataFrame:
    """Produce a node ID table from a given edge set by aggregating the source and target node IDs.

    :param edges: The edge table.
    :return: The node table with unique node IDs.
    """
    nodes_source = (edges[[COLUMN_SOURCE_ID]]).rename(columns={COLUMN_SOURCE_ID: COLUMN_DEFAULT_ID}, inplace=False)[
        [COLUMN_DEFAULT_ID]
    ]
    nodes_target = (edges[[COLUMN_TARGET_ID]]).rename(columns={COLUMN_TARGET_ID: COLUMN_DEFAULT_ID}, inplace=False)[
        [COLUMN_DEFAULT_ID]
    ]
    return pd.concat([nodes_source, nodes_target]).drop_duplicates(keep="first")


def produce_merged_node_id_list(merges: DataFrame) -> List[str]:
    """Produce a list of node IDs that are merged (the source node ID).

    :param merges: The table of merges.
    :return: The merged node IDs as a list.
    """
    return merges[COLUMN_SOURCE_ID].tolist()


def filter_nodes_for_namespace(nodes: DataFrame, namespace: str) -> DataFrame:
    """Filter a given node dataframe for a namespace.

    :param nodes: The node table to be filtered.
    :param namespace: The ontology ID.
    :return: The node dataframe where all nodes belong to the same ontology (namespace).
    """
    nodes_copy = nodes.copy()
    default_id_ns = get_namespace_column_name_for_column(COLUMN_DEFAULT_ID)
    if default_id_ns not in list(nodes_copy):
        nodes_copy = produce_table_with_namespace_column_for_node_ids(table=nodes_copy)
    nodes_for_namespace = nodes_copy.query(f'{default_id_ns} == "{namespace}"', inplace=False)
    logger.info(
        f"Found {len(nodes_for_namespace):,d} nodes for namespace " + f"{namespace} from total {len(nodes):,d} nodes."
    )
    return nodes_for_namespace


def _produce_hierarchy_edges_for_unmapped_nodes(
    unmapped_nodes: DataFrame, merges: DataFrame, source_alignment_order: List[str], hierarchy_edges: DataFrame
) -> Tuple[DataFrame, List[ConnectivityStep]]:
    # contains all merges; iteratively extended with connected nodes (where the node will "merge" to itself)
    # this provides a single data structure to identify terminus nodes in hierarchy paths, i.e. where
    # the path can be terminated while establishing connectivity to the main hierarchy
    merge_and_connectivity_map = _produce_merge_map(merges=merges)
    connectivity_order = source_alignment_order[1:]
    edges_for_all_nodes = []
    connectivity_steps = []

    for node_namespace in connectivity_order:
        # produce the hierarchy edges for the namespace node set
        (
            edges_for_namespace_nodes,
            merge_and_connectivity_map_for_ns,
            connectivity_step,
        ) = _produce_hierarchy_edges_for_unmapped_nodes_of_namespace(
            node_namespace=node_namespace,
            unmapped_nodes=unmapped_nodes,
            hierarchy_edges=hierarchy_edges,
            merge_and_connectivity_map=merge_and_connectivity_map,
        )
        connectivity_steps.append(connectivity_step)
        if edges_for_namespace_nodes:
            # update result and processing data structures
            edges_for_all_nodes.extend(edges_for_namespace_nodes)
            merge_and_connectivity_map = merge_and_connectivity_map_for_ns

    # edges
    connected_nodes = [
        node_id for node_id in unmapped_nodes[COLUMN_DEFAULT_ID].tolist() if node_id in merge_and_connectivity_map
    ]
    new_hierarchy_edges = pd.DataFrame(edges_for_all_nodes, columns=SCHEMA_MERGE_TABLE)
    logger.info(
        f"Out of {len(unmapped_nodes):,d} unmapped nodes, "
        + f"{len(connected_nodes):,d} are now connected, "
        + f"via {len(new_hierarchy_edges):,d} hierarchy edges."
    )
    return new_hierarchy_edges, connectivity_steps


def _produce_hierarchy_edges_for_unmapped_nodes_of_namespace(
    node_namespace: str, unmapped_nodes: DataFrame, hierarchy_edges: DataFrame, merge_and_connectivity_map: dict
) -> Tuple[List[Tuple[str, str]], dict, ConnectivityStep]:
    merge_and_connectivity_map_for_ns = merge_and_connectivity_map.copy()

    # get the unmapped node IDs for the namespace
    unmapped_node_ids_for_namespace = filter_nodes_for_namespace(nodes=unmapped_nodes, namespace=node_namespace)[
        COLUMN_DEFAULT_ID
    ].tolist()
    logger.info(
        f"* * * Connectivity for {node_namespace} "
        + f"({len(unmapped_node_ids_for_namespace):,d} unmapped, "
        + f"{(len(unmapped_node_ids_for_namespace) * 100) / len(unmapped_nodes):.2f}% of total) * * *"
    )
    connectivity_step = ConnectivityStep(
        source_id=node_namespace, count_unmapped_node_ids=len(unmapped_node_ids_for_namespace)
    )
    if not unmapped_node_ids_for_namespace:
        return [], {}, connectivity_step

    # get edges for ns
    edges_for_ns = mapping_utils.get_mappings_for_namespace(namespace=node_namespace, edges=hierarchy_edges)
    connectivity_step.count_available_edges = len(edges_for_ns)
    if edges_for_ns.empty:
        return [], merge_and_connectivity_map_for_ns, connectivity_step

    # create the hierarchy graph for the namespace
    hierarchy_graph_for_ns = NetworkitGraph(edges=edges_for_ns)
    reachable_nodes = list(hierarchy_graph_for_ns.node_id_to_index_map.keys())
    reachable_unmapped_nodes = [node_id for node_id in unmapped_node_ids_for_namespace if node_id in reachable_nodes]
    count_unmapped = len(reachable_unmapped_nodes)
    connectivity_step.count_reachable_unmapped_nodes = count_unmapped
    logger.info(
        f"Reachable unmapped nodes {len(reachable_unmapped_nodes):,d} "
        + f"({(len(reachable_unmapped_nodes) * 100) / len(unmapped_node_ids_for_namespace):.2f}%)"
    )

    # connect each reachable node
    edges_for_namespace_nodes = []
    counter = 1
    for node_to_connect in reachable_unmapped_nodes:
        _progress_bar(count=counter, total=count_unmapped, status=f" Connecting {node_namespace} ")
        counter += 1
        if node_to_connect not in merge_and_connectivity_map:
            edges_for_node = _produce_hierarchy_path_for_unmapped_node(
                node_to_connect=node_to_connect,
                unmapped_node_ids=unmapped_node_ids_for_namespace,
                merge_and_connectivity_map_for_ns=merge_and_connectivity_map_for_ns,
                hierarchy_graph_for_ns=hierarchy_graph_for_ns,
            )
            if edges_for_node:
                # update result and processing data structures
                edges_for_namespace_nodes.extend(edges_for_node)
                merge_and_connectivity_map_for_ns.update(
                    {item: item for item in list(itertools.chain(*edges_for_node))[0:-1]}
                )

    # results
    connected_nodes = [
        node_id for node_id in unmapped_node_ids_for_namespace if node_id in merge_and_connectivity_map_for_ns
    ]
    connectivity_step.count_connected_nodes = len(connected_nodes)
    connectivity_step.count_produced_edges = len(edges_for_namespace_nodes)
    logger.info(
        f"Out of {len(unmapped_node_ids_for_namespace):,d} unmapped nodes of '{node_namespace}', "
        + f"{len(connected_nodes):,d} are now connected, "
        + f"via {len(edges_for_namespace_nodes):,d} hierarchy edges."
    )

    return edges_for_namespace_nodes, merge_and_connectivity_map_for_ns, connectivity_step


def _produce_hierarchy_path_for_unmapped_node(
    node_to_connect: str,
    unmapped_node_ids: List[str],
    merge_and_connectivity_map_for_ns: dict,
    hierarchy_graph_for_ns: NetworkitGraph,
) -> List[Tuple[str, str]]:
    # get shortest path
    shortest_path = hierarchy_graph_for_ns.get_path_for_node(node_id=node_to_connect)
    if not shortest_path:
        return []

    # check if it contains any merged nodes, i.e. whether it can be used for integration
    merged_node_ids_in_path = [
        node_id for node_id in shortest_path if node_id in merge_and_connectivity_map_for_ns.keys()
    ]
    if not merged_node_ids_in_path:
        return []

    # modify the path: removed redundant (no unmapped nodes), terminate it with a merged node
    # and use the canonical ID for the terminus node
    index_of_first_merged_node = sorted([shortest_path.index(node_id) for node_id in merged_node_ids_in_path])[0]
    first_merged_node_canonical_id = merge_and_connectivity_map_for_ns[shortest_path[index_of_first_merged_node]]
    pruned_path = shortest_path[0:index_of_first_merged_node] + [first_merged_node_canonical_id]
    unmapped_node_ids_in_path = [node_id for node_id in shortest_path if node_id in unmapped_node_ids]
    permitted_node_ids_in_path = unmapped_node_ids_in_path + merged_node_ids_in_path + [first_merged_node_canonical_id]
    final_path = [node_id for node_id in pruned_path if node_id in permitted_node_ids_in_path]

    # convert the path into a hierarchy edge tuple list
    edges = _convert_hierarchy_path_into_tuple_list(pruned_path=final_path)

    return edges


def _produce_merge_map(merges: DataFrame) -> dict:
    return {row[COLUMN_SOURCE_ID]: row[COLUMN_TARGET_ID] for _, row in merges.iterrows()}


def _convert_hierarchy_path_into_tuple_list(pruned_path: List[str]) -> List[Tuple[str, str]]:
    return [
        (pruned_path[source_index], pruned_path[source_index + 1]) for source_index in range(0, (len(pruned_path) - 1))
    ]


def _progress_bar(count, total, status=""):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.1 * count / float(total), 2)
    bar = "=" * filled_len + "-" * (bar_len - filled_len)
    sys.stdout = sys.__stdout__
    sys.stdout.write("[%s] %s%s ...%s\r" % (bar, percents, "%", status))
    sys.stdout.flush()


def _convert_connectivity_steps_to_named_table(
    steps: List[ConnectivityStep],
) -> NamedTable:
    """Convert the list of ConnectivityStep dataclasses to a named table.

    :param steps: The list of ConnectivityStep dataclasses.
    :return: The ConnectivityStep report dataframe wrapped as a named table.
    """
    return NamedTable(
        TABLE_CONNECTIVITY_STEPS_REPORT,
        pd.DataFrame(
            [dataclasses.astuple(step) for step in steps],
            columns=SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE,
        ),
    )
