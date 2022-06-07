"""Helper methods for data file processing and analysis."""

from typing import List

import pandas as pd
from pandas import DataFrame

from onto_merger.data.constants import (
    COLUMN_COUNT,
    COLUMN_DEFAULT_ID,
    COLUMN_FREQUENCY,
    COLUMN_NAMESPACE,
    COLUMN_SOURCE_ID,
    COLUMN_SOURCE_TO_TARGET,
    COLUMN_TARGET_ID,
    NODE_ID_COLUMNS,
    SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE,
)
from onto_merger.data.dataclasses import NamedTable
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def get_namespace_column_name_for_column(node_id_column: str) -> str:
    """Produce a column name for a given node ID column name.

    :param node_id_column: The name of the node ID column.
    :return: The namespace column name.
    """
    return f"namespace_{node_id_column}"


def get_namespace_for_node_id(node_id: str) -> str:
    """Get the namespace part of a node ID.

    :param node_id: The node ID.
    :return: The node ID namespace.
    """
    return node_id.split(":")[0]


def produce_table_with_namespace_column_for_node_ids(table: DataFrame) -> DataFrame:
    """Produce a table with a namespace column for all node ID columns.

    In node tables there is only one node ID column. In edge tables
    (hierarchy, mappings, merges) there are always two node ID columns.

    :param table: The table to be appended with namespace column(s).
    :return: A new table with a corresponding namespace column for all node ID columns.
    """
    if len(table) == 0:
        return table
    table_copy = table.copy()
    table_node_id_columns = sorted([col_name for col_name in NODE_ID_COLUMNS if col_name in list(table_copy)])
    for node_id_column in table_node_id_columns:
        namespace_column_name = get_namespace_column_name_for_column(node_id_column=node_id_column)
        if namespace_column_name not in list(table_copy):
            table_copy[namespace_column_name] = table_copy[node_id_column].apply(
                lambda node_id: get_namespace_for_node_id(str(node_id))
            )
    return table_copy


def produce_table_with_namespace_column_pair(table: DataFrame) -> DataFrame:
    """Produce a table with a single column representing the source and target node ID namespace.

    :param table: The table to be appended.
    :return: The same table if it is a node table, a new table with the appended
    column if it is an edge table.
    """
    if len(table) == 0:
        return table
    if COLUMN_TARGET_ID not in list(table):
        return table
    if COLUMN_SOURCE_TO_TARGET in list(table):
        return table
    table_copy = table.copy()
    table_copy[COLUMN_SOURCE_TO_TARGET] = table_copy.apply(
        lambda x: f"{get_namespace_for_node_id(x[COLUMN_SOURCE_ID])}"
                  + f" to {get_namespace_for_node_id(x[COLUMN_TARGET_ID])}",
        axis=1,
    )
    return table_copy


def add_namespace_column_to_loaded_tables(
        tables: List[NamedTable],
) -> List[NamedTable]:
    """Produce a list of named tables appended with namespace columns, and namespace pair column.

    :param tables: The list of named tables to be appended.
    :return: The list of named tables appended with namespace and namespace pair
    columns.
    """
    return [
        NamedTable(
            name=table.name,
            dataframe=produce_table_with_namespace_column_pair(
                produce_table_with_namespace_column_for_node_ids(table.dataframe)
            ),
        )
        for table in tables
    ]


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


def produce_table_node_ids_from_edge_table(edges: DataFrame) -> DataFrame:
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


def produce_table_node_namespace_distribution(
        node_table: DataFrame,
) -> DataFrame:
    """Produce a named table that shows the node ID namespaces distribution of the input table.

    :param node_table: A node table.
    :return: The analysis table showing the node ID namespaces distribution of the
    input.
    """
    node_table_count = len(node_table)
    if node_table_count == 0:
        return pd.DataFrame([], columns=[SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE])

    ns_column = get_namespace_column_name_for_column(COLUMN_DEFAULT_ID)
    if ns_column not in list(node_table):
        node_table = produce_table_with_namespace_column_for_node_ids(node_table)

    # count per NS, descending, with ratio of total
    namespace_distribution_table = node_table\
        .groupby([ns_column])\
        .count()\
        .reset_index()\
        .sort_values(COLUMN_DEFAULT_ID, ascending=False)\
        .rename(columns={COLUMN_DEFAULT_ID: COLUMN_COUNT, ns_column: COLUMN_NAMESPACE})

    namespace_distribution_table[COLUMN_FREQUENCY] = namespace_distribution_table.apply(
        lambda x: f"{((x[COLUMN_COUNT] / node_table_count) * 100):.2f}%", axis=1
    )
    return namespace_distribution_table[SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE]
