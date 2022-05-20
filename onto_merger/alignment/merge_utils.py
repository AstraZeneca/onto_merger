"""Helper methods for producing the node merge table."""

from typing import List, Optional

import numpy as np
import pandas as pd
from networkx import connected_components
from pandas import DataFrame

from onto_merger.alignment import networkx_utils
from onto_merger.analyser import analysis_utils
from onto_merger.data.constants import (
    COLUMN_MAPPING_TYPE_GROUP,
    COLUMN_SOURCE_ID,
    COLUMN_SOURCE_ID_ALIGNED_TO,
    COLUMN_STEP_COUNTER,
    COLUMN_TARGET_ID,
    SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    SCHEMA_MERGE_TABLE_WITH_META_DATA,
    TABLE_MERGES_AGGREGATED,
    TABLE_MERGES_WITH_META_DATA,
    TABLE_NODES_MERGED, COLUMN_DEFAULT_ID, TABLE_NODES_UNMAPPED, TABLE_NODES, SCHEMA_NODE_ID_LIST_TABLE,
    TABLE_NODES_DOMAIN, COLUMN_RELATION, COLUMN_PROVENANCE, TABLE_NAME_TO_TABLE_SCHEMA_MAP, TABLE_MERGES_DOMAIN,
    ONTO_MERGER, RELATION_MERGE)
from onto_merger.data.dataclasses import DataRepository, NamedTable
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def post_process_alignment_results(data_repo: DataRepository,
                                   alignment_priority_order: List[str]) -> List[NamedTable]:
    # aggregate merges
    table_aggregated_merges = _produce_named_table_aggregated_merges(
        merges=data_repo.get(TABLE_MERGES_WITH_META_DATA).dataframe,
        alignment_priority_order=alignment_priority_order,
    )
    # nodes
    table_merged_nodes = _produce_named_table_merged_nodes(merges_aggregated=table_aggregated_merges.dataframe)
    table_unmapped_nodes = _produce_named_table_unmapped_nodes(
        nodes=data_repo.get(TABLE_NODES).dataframe,
        merged_nodes=table_merged_nodes.dataframe
    )

    return [table_aggregated_merges, table_merged_nodes, table_unmapped_nodes]


def _produce_named_table_aggregated_merges(merges: DataFrame, alignment_priority_order: List[str]) -> NamedTable:
    """Produce a named table with aggregated merges.

    In aggregated merges the target ID is always the canonical ID for a given merge cluster
    (e.g. A -> B, B -> C becomes A -> C and B -> C, where the priority order is C, B, A).

    :param merges: The set of input merges.
    :param alignment_priority_order: The alignment priority order that defines the
    canonical node.
    :return: The set of aggregated merges.
    """
    # create graph, find sub-graphs i.e. mapping clusters and convert it into
    # a dataframe
    graph = networkx_utils.create_networkx_graph(edges=merges[SCHEMA_EDGE_SOURCE_TO_TARGET_IDS])
    sub_graphs = list(graph.subgraph(c) for c in connected_components(graph))
    clusters = [list(sub.nodes) for sub in sub_graphs]
    merges_aggregated = pd.DataFrame([[i] for i in np.array(clusters)])
    merges_aggregated.columns = [COLUMN_SOURCE_ID]

    # canonical node according to the priority order
    merges_aggregated[COLUMN_TARGET_ID] = merges_aggregated.apply(
        lambda x: _get_canonical_node_for_merge_cluster(
            merge_cluster=x[COLUMN_SOURCE_ID],
            alignment_priority_order=alignment_priority_order,
        ),
        axis=1,
    )

    # convert to merge table
    merges_aggregated = merges_aggregated.explode(column=COLUMN_SOURCE_ID).sort_values(
        [COLUMN_TARGET_ID, COLUMN_SOURCE_ID]
    )[SCHEMA_EDGE_SOURCE_TO_TARGET_IDS]
    merges_aggregated.query(expr=f"{COLUMN_SOURCE_ID} != {COLUMN_TARGET_ID}", inplace=True)

    return NamedTable(TABLE_MERGES_AGGREGATED, merges_aggregated)


def _produce_named_table_merged_nodes(merges_aggregated: DataFrame) -> NamedTable:
    """Produce a named table by wrapping merge dataframe.

    :param merges_aggregated: The merge dataframe.
    :return: The named table.
    """
    return NamedTable(
        name=TABLE_NODES_MERGED,
        dataframe=_produce_table_merged_nodes(merges=merges_aggregated),
    )


def _produce_table_merged_nodes(merges: DataFrame) -> DataFrame:
    return merges[[COLUMN_SOURCE_ID]] \
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_DEFAULT_ID}, inplace=False) \
        .sort_values([COLUMN_DEFAULT_ID], ascending=True)


def _produce_named_table_unmapped_nodes(nodes: DataFrame, merged_nodes: DataFrame) -> NamedTable:
    """Produce the dataframe of unmapped node IDs.

    :param nodes: The set of input nodes to be filtered.
    :param merged_nodes: The set of merges used to determine node mapped status.
    :return: The set of unmapped nodes.
    """
    df = pd.concat([nodes[[COLUMN_DEFAULT_ID]], merged_nodes, merged_nodes]).drop_duplicates(keep=False)
    logger.info(
        f"Out of {len(nodes):,d} nodes, {len(df):,d} "
        + f"({((len(df) / len(nodes)) * 100):.2f}%) are unmapped."
    )
    return NamedTable(TABLE_NODES_UNMAPPED, df)


def produce_named_table_merges_with_alignment_meta_data(
        merges: DataFrame, source_id: str, step_counter: int, mapping_type: str
) -> NamedTable:
    """Produce a named merge table with alignment meta data (step number, mapping types used).

    :param merges: The set of merges produced in the alignment step.
    :param source_id: The source ontology nodes are being merged onto.
    :param step_counter: The alignment step number.
    :param mapping_type: The mapping type used in the alignment step.
    :return:
    """
    df = merges.copy()
    df[COLUMN_SOURCE_ID_ALIGNED_TO] = source_id
    df[COLUMN_STEP_COUNTER] = step_counter
    df[COLUMN_MAPPING_TYPE_GROUP] = mapping_type
    return NamedTable(
        name=TABLE_MERGES_WITH_META_DATA,
        dataframe=df[SCHEMA_MERGE_TABLE_WITH_META_DATA],
    )


def produce_named_table_domain_nodes(nodes: DataFrame, merged_nodes: DataFrame,) -> NamedTable:
    df = pd.concat([
        nodes[SCHEMA_NODE_ID_LIST_TABLE],
        merged_nodes[SCHEMA_NODE_ID_LIST_TABLE],
        merged_nodes[SCHEMA_NODE_ID_LIST_TABLE]
    ]).drop_duplicates(keep="first")
    return NamedTable(TABLE_NODES_DOMAIN, df)


def produce_named_table_domain_merges(merges_aggregated: DataFrame,) -> NamedTable:
    df = merges_aggregated.copy()
    df[COLUMN_RELATION] = RELATION_MERGE
    df[COLUMN_PROVENANCE] = ONTO_MERGER
    df = df[TABLE_NAME_TO_TABLE_SCHEMA_MAP[TABLE_MERGES_DOMAIN]]
    return NamedTable(TABLE_MERGES_DOMAIN, df)


def produce_table_unmapped_nodes(nodes: DataFrame, merges: DataFrame) -> DataFrame:
    """Produce the dataframe of unmapped node IDs.

    :param merges:
    :param nodes: The set of input nodes to be filtered.
    :return: The set of unmapped nodes.
    """
    merged_nodes = _produce_table_merged_nodes(merges=merges)
    df = pd.concat([nodes[[COLUMN_DEFAULT_ID]], merged_nodes, merged_nodes]).drop_duplicates(keep=False)
    logger.info(
        f"Out of {len(nodes):,d} nodes, {len(df):,d} "
        + f"({((len(df) / len(nodes)) * 100):.2f}%) are unmapped."
    )
    return df


def _get_canonical_node_for_merge_cluster(
        merge_cluster: List[str], alignment_priority_order: List[str]
) -> Optional[str]:
    """Return the canonical node ID for a given merge cluster.

    :param merge_cluster: The merge cluster, i.e. nodes that form a merge chain.
    :param alignment_priority_order: The alignment priority order that defines the
    canonical node.
    :return: The canonical node ID if it can be determined, otherwise None.
    """
    merge_cluster_ns_to_id = {analysis_utils.get_namespace_for_node_id(node_id): node_id for node_id in merge_cluster}
    for source_id in alignment_priority_order:
        if source_id in merge_cluster_ns_to_id:
            return merge_cluster_ns_to_id.get(source_id)
    return None
