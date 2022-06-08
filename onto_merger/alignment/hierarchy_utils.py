"""Methods to produce node hierarchy and analyse node connectivity status."""

import itertools
from typing import List, Optional, Tuple

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from onto_merger.alignment import networkx_utils
from onto_merger.alignment.networkit_utils import NetworkitGraph
from onto_merger.analyser.analysis_utils import (
    filter_nodes_for_namespace,
    produce_table_node_ids_from_edge_table,
)
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    ONTO_MERGER,
    RELATION_RDFS_SUBCLASS_OF,
    SCHEMA_HIERARCHY_EDGE_TABLE,
    SCHEMA_MERGE_TABLE,
    SCHEMA_NODE_ID_LIST_TABLE,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MERGES_AGGREGATED,
    TABLE_NODES,
    TABLE_NODES_CONNECTED,
    TABLE_NODES_CONNECTED_EXC_SEED,
    TABLE_NODES_DANGLING,
    TABLE_NODES_SEED,
    TABLE_NODES_UNMAPPED,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import (
    AlignmentConfig,
    ConnectivityStep,
    DataRepository,
    NamedTable,
    convert_connectivity_steps_to_named_table,
)
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


class HierarchyManager:
    """Connect domain ontology nodes to form a single DAG."""

    def __init__(self, data_manager: DataManager):
        """Initialise the HierarchyManager class.

        :param data_manager: The data manager instance used to perform data operations and produce file paths.
        """
        self.data_manager = data_manager
        self.f = open(self.data_manager.get_hierarchy_edges_paths_debug_file_path(), "w")
        self.f.write("connected_node_id,connected_node_ns,length_original_path,"
                     + "length_produced_path,original_path,produced_path,"
                     + "index_of_first_merged_node_in_org_path,first_merged_node_canonical_id\n")

    def connect_nodes(
            self, alignment_config: AlignmentConfig, source_alignment_order: List[str], data_repo: DataRepository
    ) -> List[NamedTable]:
        """Run the connectivity process to establish a hierarchy between the domain nodes.

        :param source_alignment_order: The source alignment order.
        :param alignment_config: The alignment process configuration dataclass.
        :param data_repo: The data repository containing the input data.
        :return: The produced hierarchy edge table.
        """
        logger.info(f"{('* ' * 20)}")
        logger.info(f"Input nodes {len(data_repo.get(TABLE_NODES).dataframe):,d}"
                    + f" | Unmapped nodes {len(data_repo.get(TABLE_NODES_UNMAPPED).dataframe):,d}")

        # (1) get the seed hierarchy as main scaffolding
        seed_hierarchy_df = _produce_table_seed_ontology_hierarchy(
            seed_ontology_name=alignment_config.base_config.seed_ontology_name,
            nodes=data_repo.get(TABLE_NODES).dataframe,
            hierarchy_edges=data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
        )

        # (2) connect unmapped nodes to the seed hierarchy
        unmapped_node_hierarchy_df, connectivity_steps = self._produce_hierarchy_edges_for_unmapped_nodes(
            unmapped_nodes=data_repo.get(TABLE_NODES_UNMAPPED).dataframe,
            merges=data_repo.get(TABLE_MERGES_AGGREGATED).dataframe,
            source_alignment_order=source_alignment_order,
            hierarchy_edges=data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
        )

        # (4) return the merged hierarchy and node tables
        return [
            NamedTable(
                name=TABLE_EDGES_HIERARCHY_POST,
                dataframe=pd.concat([seed_hierarchy_df, unmapped_node_hierarchy_df]).drop_duplicates(keep="first"),
            ),
            convert_connectivity_steps_to_named_table(steps=connectivity_steps),
        ]

    def _produce_hierarchy_edges_for_unmapped_nodes(
            self, unmapped_nodes: DataFrame, merges: DataFrame, source_alignment_order: List[str],
            hierarchy_edges: DataFrame
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
            ) = self._produce_hierarchy_edges_for_unmapped_nodes_of_namespace(
                node_namespace=node_namespace,
                unmapped_nodes=unmapped_nodes,
                hierarchy_edges=hierarchy_edges,
                merge_and_connectivity_map=merge_and_connectivity_map,
            )
            connectivity_step.step_counter = connectivity_order.index(node_namespace)
            connectivity_steps.append(connectivity_step)
            if edges_for_namespace_nodes:
                # update result and processing data structures
                edges_for_all_nodes.extend(edges_for_namespace_nodes)
                merge_and_connectivity_map = merge_and_connectivity_map_for_ns
        self.f.close()

        # edges
        new_hierarchy_edges = _produce_hierarchy_edge_table_from_edge_path_lists(
            edges_for_all_nodes=edges_for_all_nodes)
        connected_nodes = produce_table_node_ids_from_edge_table(edges=new_hierarchy_edges)

        logger.info(
            f"Out of {len(unmapped_nodes):,d} unmapped nodes, "
            + f"{len(connected_nodes):,d} are now connected, "
            + f"via {len(new_hierarchy_edges):,d} hierarchy edges."
        )
        return new_hierarchy_edges, connectivity_steps

    def _produce_hierarchy_edges_for_unmapped_nodes_of_namespace(
            self, node_namespace: str, unmapped_nodes: DataFrame, hierarchy_edges: DataFrame,
            merge_and_connectivity_map: dict,
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
            source_id=node_namespace, count_unmapped_node_ids=len(unmapped_node_ids_for_namespace),
        )
        if not unmapped_node_ids_for_namespace:
            connectivity_step.task_finished()
            return [], {}, connectivity_step

        # get edges for ns
        edges_for_ns = hierarchy_edges.query(expr=f"prov == '{node_namespace}'", inplace=False)
        connectivity_step.count_available_edges = len(edges_for_ns)
        if edges_for_ns.empty:
            connectivity_step.task_finished()
            return [], merge_and_connectivity_map_for_ns, connectivity_step

        # create the hierarchy graph for the namespace
        hierarchy_graph_for_ns = NetworkitGraph(edges=edges_for_ns)
        reachable_nodes = list(hierarchy_graph_for_ns.node_id_to_index_map.keys())
        reachable_unmapped_nodes = [node_id for node_id in unmapped_node_ids_for_namespace if
                                    node_id in reachable_nodes]
        count_unmapped = len(reachable_unmapped_nodes)
        connectivity_step.count_reachable_unmapped_nodes = count_unmapped
        logger.info(
            f"Reachable unmapped nodes {len(reachable_unmapped_nodes):,d} "
            + f"({(len(reachable_unmapped_nodes) * 100) / len(unmapped_node_ids_for_namespace):.2f}%)\n"
        )

        # connect each reachable node
        edges_for_namespace_nodes = []
        counter = 1
        with tqdm(total=count_unmapped, desc=f"Connecting {node_namespace} nodes") as progress_bar:
            for node_to_connect in reachable_unmapped_nodes:
                counter += 1
                if node_to_connect not in merge_and_connectivity_map:
                    edges_for_node = self._produce_hierarchy_path_for_unmapped_node(
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
                progress_bar.update(1)

        # results
        connected_nodes = produce_table_node_ids_from_edge_table(
            edges=_produce_hierarchy_edge_table_from_edge_path_lists(
                edges_for_all_nodes=edges_for_namespace_nodes
            )
        )
        connectivity_step.count_connected_nodes = len(connected_nodes)
        connectivity_step.count_produced_edges = len(edges_for_namespace_nodes)
        logger.info(
            f"Out of {len(unmapped_node_ids_for_namespace):,d} unmapped nodes of '{node_namespace}', "
            + f"{len(connected_nodes):,d} are now connected, "
            + f"via {len(edges_for_namespace_nodes):,d} hierarchy edges."
        )
        connectivity_step.task_finished()

        return edges_for_namespace_nodes, merge_and_connectivity_map_for_ns, connectivity_step

    def _produce_hierarchy_path_for_unmapped_node(
            self,
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
        permitted_node_ids_in_path = unmapped_node_ids_in_path + merged_node_ids_in_path + [
            first_merged_node_canonical_id]
        final_path = [node_id for node_id in pruned_path if node_id in permitted_node_ids_in_path]

        #
        self.f.write(f"{node_to_connect},{node_to_connect.split(':')[0]},{len(shortest_path)},{len(final_path)},"
                     + f"{str(shortest_path).replace(',', '')},{str(final_path).replace(',', '')}, "
                     + f"{index_of_first_merged_node},{first_merged_node_canonical_id}\n")

        # convert the path into a hierarchy edge tuple list
        edges = _convert_hierarchy_path_into_tuple_list(pruned_path=final_path)

        return edges


def _produce_table_seed_ontology_hierarchy(
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
        return seed_hierarchy_table[SCHEMA_HIERARCHY_EDGE_TABLE]


def _produce_merge_map(merges: DataFrame) -> dict:
    return {row[COLUMN_SOURCE_ID]: row[COLUMN_TARGET_ID] for _, row in merges.iterrows()}


def _convert_hierarchy_path_into_tuple_list(pruned_path: List[str]) -> List[Tuple[str, str]]:
    return [
        (pruned_path[source_index], pruned_path[source_index + 1]) for source_index in range(0, (len(pruned_path) - 1))
    ]


def _produce_hierarchy_edge_table_from_edge_path_lists(edges_for_all_nodes: List[Tuple[str, str]]) -> DataFrame:
    new_hierarchy_edges = pd.DataFrame(edges_for_all_nodes, columns=list(SCHEMA_MERGE_TABLE))
    new_hierarchy_edges[COLUMN_RELATION] = RELATION_RDFS_SUBCLASS_OF
    new_hierarchy_edges[COLUMN_PROVENANCE] = ONTO_MERGER
    new_hierarchy_edges = new_hierarchy_edges[SCHEMA_HIERARCHY_EDGE_TABLE]
    return new_hierarchy_edges


def post_process_connectivity_results(data_repo: DataRepository) -> List[NamedTable]:
    """Produce tables for analysing the connectivity results.

    :param data_repo: The data repository containing the produced tables.
    :return: The produced named tables.
    """
    nodes_connected = produce_named_table_nodes_connected(
        hierarchy_edges=data_repo.get(TABLE_EDGES_HIERARCHY_POST).dataframe
    )
    nodes_connected_excluding_seed = _produce_named_table_nodes_connected_excluding_seed(
        nodes_connected=nodes_connected.dataframe,
        nodes_seed=data_repo.get(TABLE_NODES_SEED).dataframe,
    )
    nodes_dangling = produce_named_table_nodes_dangling(
        nodes_all=data_repo.get(TABLE_NODES_UNMAPPED).dataframe,
        nodes_connected=nodes_connected.dataframe,
    )
    return [nodes_connected, nodes_dangling, nodes_connected_excluding_seed]


def produce_named_table_nodes_connected(hierarchy_edges: DataFrame) -> NamedTable:
    """Produce a table containing nodes that are in an edge hierarchy.

    :param hierarchy_edges: The domain node hierarchy.
    :return: The table of nodes connected that are not merged but connected.
    """
    df = produce_table_node_ids_from_edge_table(edges=hierarchy_edges)[SCHEMA_NODE_ID_LIST_TABLE]
    logger.info(
        f"There are {len(df):,d} connected nodes (hierarchy edges = {len(hierarchy_edges):,d})."
    )
    return NamedTable(TABLE_NODES_CONNECTED, df)


def _produce_named_table_nodes_connected_excluding_seed(
        nodes_connected: DataFrame, nodes_seed: DataFrame,
) -> NamedTable:
    df = pd.concat([nodes_connected, nodes_seed, nodes_seed]).drop_duplicates(keep=False)
    logger.info(
        f"There are {len(nodes_connected):,d} connected nodes, {len(df):,d} excluding seed)."
    )
    return NamedTable(TABLE_NODES_CONNECTED_EXC_SEED, df)


def produce_named_table_nodes_dangling(
        nodes_all: DataFrame, nodes_connected: DataFrame,
) -> NamedTable:
    """Produce a table containing nodes that are not merged or connected.

    :param nodes_all: The set of all nodes.
    :param nodes_connected:  The set of all connected nodes.
    :return: The table of nodes connected that are not merged but connected.
    """
    df = pd.concat([
        nodes_all[SCHEMA_NODE_ID_LIST_TABLE],
        nodes_connected[SCHEMA_NODE_ID_LIST_TABLE],
        nodes_connected[SCHEMA_NODE_ID_LIST_TABLE],
    ]).drop_duplicates(keep=False)
    logger.info(
        f"Out of {len(nodes_all):,d} nodes, {len(df):,d} "
        + f"({((len(df) / len(nodes_all)) * 100):.2f}%) are dangling."
    )
    return NamedTable(TABLE_NODES_DANGLING, df)
