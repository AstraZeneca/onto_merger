from typing import List, Optional

import numpy as np
import pandas as pd
from networkx import connected_components
from pandas import DataFrame

from onto_merger.alignment import networkx_utils
from onto_merger.analyser import analysis_util
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_MAPPING_TYPE_GROUP,
    COLUMN_SOURCE_ID,
    COLUMN_SOURCE_ID_ALIGNED_TO,
    COLUMN_STEP_COUNTER,
    COLUMN_TARGET_ID,
    SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    SCHEMA_MERGE_TABLE_WITH_META_DATA,
    TABLE_MERGES,
    TABLE_MERGES_AGGREGATED,
    TABLE_NODES_MERGED,
)
from onto_merger.data.dataclasses import NamedTable
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def produce_named_table_aggregate_merges(merges: DataFrame, alignment_priority_order: List[str]) -> NamedTable:
    """Produces a named table with aggregated merges, i.e. merges where the target ID
    is always the canonical ID for a given merge cluster (e.g. A -> B, B -> C becomes
    A -> C and B -> C, where the priorirty order is C, B, A).

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
        lambda x: get_canonical_node_for_merge_cluster(
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


def get_canonical_node_for_merge_cluster(
    merge_cluster: List[str], alignment_priority_order: List[str]
) -> Optional[str]:
    """Returns the canonical node ID for a given merge cluster.

    :param merge_cluster: The merge cluster, i.e. nodes that form a merge chain.
    :param alignment_priority_order: The alignment priority order that defines the
    canonical node.
    :return: The canonical node ID if it can be determined, otherwise None.
    """
    merge_cluster_ns_to_id = {analysis_util.get_namespace_for_node_id(node_id): node_id for node_id in merge_cluster}
    for source_id in alignment_priority_order:
        if source_id in merge_cluster_ns_to_id:
            return merge_cluster_ns_to_id.get(source_id)
    return None


def produce_named_table_merged_nodes(merges: DataFrame) -> NamedTable:
    """Produces a named table by wrapping merge dataframe.

    :param merges: The merge dataframe.
    :return: The named table.
    """
    return NamedTable(
        name=TABLE_NODES_MERGED,
        dataframe=merges[[COLUMN_SOURCE_ID]]
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_DEFAULT_ID}, inplace=False)
        .sort_values([COLUMN_DEFAULT_ID], ascending=True),
    )


def produce_named_table_merges_with_alignment_meta_data(
    merges: DataFrame, source_id: str, step_counter: int, mapping_type: str
) -> NamedTable:
    """Produces a named merge table with alignment meta data (step number, mapping
    types used).

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
        name=TABLE_MERGES,
        dataframe=df[SCHEMA_MERGE_TABLE_WITH_META_DATA],
    )
