from typing import List
import itertools

import pandas as pd
from pandas import DataFrame

from onto_merger.data.data_manager import DataManager
from onto_merger.analyser.analysis_utils import produce_table_with_namespace_column_for_node_ids
from onto_merger.analyser.constants import COLUMN_NAMESPACE_TARGET_ID, COLUMN_NAMESPACE_SOURCE_ID
from onto_merger.alignment.hierarchy_utils import produce_named_table_nodes_connected, \
    produce_named_table_nodes_dangling
from onto_merger.data.constants import COLUMN_TARGET_ID, COLUMN_SOURCE_ID, TABLE_EDGES_HIERARCHY, \
    TABLE_EDGES_HIERARCHY_POST, TABLE_NODES, TABLE_NODES_DOMAIN, SCHEMA_NODE_ID_LIST_TABLE, COLUMN_DEFAULT_ID, \
    TABLE_NODES_CONNECTED, TABLE_NODES_DANGLING, COLUMN_PROVENANCE
from onto_merger.data.dataclasses import NamedTable, DataRepository


# HELPERS #
def _get_percentage_of(subset_count: int, total_count: int):
    return round((subset_count / total_count * 100), 2)


# MERGE #
def produce_merge_cluster_analysis(merges_aggregated: DataFrame) -> List[NamedTable]:
    column_cluster_size = 'cluster_size'
    column_many_to_one_nss = 'many_to_one_nss'
    column_many_to_one_nss_size = f"{column_many_to_one_nss}_size"

    # clusters
    df = merges_aggregated.copy()
    df = df[[COLUMN_TARGET_ID, COLUMN_SOURCE_ID,
             COLUMN_NAMESPACE_TARGET_ID, COLUMN_NAMESPACE_SOURCE_ID]] \
        .groupby([COLUMN_TARGET_ID, COLUMN_NAMESPACE_TARGET_ID]) \
        .agg(cluster_size=(COLUMN_SOURCE_ID, lambda x: len(list(x)) + 1),
             merged_nss_unique_count=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: len(set(x))),
             merged_nss_count=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: len(list(x))),
             merged_nss_unique=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: set(x)),
             merged_nss=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: list(x))) \
        .reset_index() \
        .sort_values(column_cluster_size, ascending=False)

    # describe: cluster_size
    df_cluster_size_describe = df[[column_cluster_size]] \
        .describe() \
        .reset_index(level=0)

    # cluster size | x: size bins | y: freq
    df_bins = df[[column_cluster_size]] \
        .groupby([column_cluster_size]) \
        .agg(size_bin=(column_cluster_size, 'count')) \
        .reset_index()

    # namespaces in clusters
    df[column_many_to_one_nss] = df.apply(lambda x: set([key
                                                         for key, group in itertools.groupby(x['merged_nss'])
                                                         if len(list(group)) > 1]), axis=1)
    df[column_many_to_one_nss_size] = df.apply(lambda x: len(x[column_many_to_one_nss]), axis=1)
    df_many_nss_merged_to_one = df[[COLUMN_TARGET_ID, column_many_to_one_nss, column_many_to_one_nss_size]]
    df_many_nss_merged_to_one = df_many_nss_merged_to_one[df_many_nss_merged_to_one[column_many_to_one_nss_size] > 0]
    members = df_many_nss_merged_to_one[column_many_to_one_nss].tolist()
    flat_list = list(set([item for sublist in members for item in sublist]))
    for ns in flat_list:
        df_many_nss_merged_to_one[ns] = \
            df_many_nss_merged_to_one.apply(lambda x: 1 if ns in x[column_many_to_one_nss] else 0, axis=1)
    df_many_nss_merged_to_one = df_many_nss_merged_to_one.sort_values(column_many_to_one_nss_size, ascending=False)
    nss_data = [
        (ns, df_many_nss_merged_to_one[ns].sum())
        for ns in flat_list
    ]
    df_nss_analysis = pd.DataFrame(nss_data, columns=["namespace", "count_occurs_multiple_times_in_cluster"]) \
        .sort_values("count_occurs_multiple_times_in_cluster", ascending=False)

    # top 10 merge clusters per NS
    top_ten_merge_clusters_per_ns = [
        NamedTable(f"clusters_top10_for_{ns}", df.query(f"namespace_target_id == '{ns}'", inplace=False).head(10))
        for ns in df['namespace_target_id']
    ]

    tables = [
        NamedTable("clusters", df),
        NamedTable("cluster_size_description", df_cluster_size_describe),
        NamedTable("cluster_size_bin_freq", df_bins),
        NamedTable("many_nss_merged_to_one", df_many_nss_merged_to_one),
        NamedTable("many_nss_merged_to_one_freq", df_nss_analysis),
    ]
    tables.extend(top_ten_merge_clusters_per_ns)

    return tables


# HIERARCHY EDGE PATHS
def produce_hierarchy_edge_path_analysis(hierarchy_edges_paths: DataFrame) -> List[NamedTable]:
    # compute path diff
    df = hierarchy_edges_paths[["connected_node_ns", "length_original_path", "length_produced_path"]].copy()
    df["path_diff"] = df.apply(lambda x: (x["length_original_path"] - x["length_produced_path"]), axis=1)

    # describe: cluster_size
    tables = [
        NamedTable("path_lengths", df),
        NamedTable("path_lengths_description_ALL", _describe_hierarchy_edge_path_lengths(df=df))
    ]
    for ns in sorted(list(set(df["connected_node_ns"].tolist()))):
        df_for_ns = df.query(f"connected_node_ns == '{ns}'", inplace=False)
        tables.append(
            NamedTable(f"path_lengths_description_{ns}", _describe_hierarchy_edge_path_lengths(df=df_for_ns))
        )

    return tables


def _describe_hierarchy_edge_path_lengths(df: DataFrame) -> DataFrame:
    df_path_size_describe = df[["length_original_path", "length_produced_path", "path_diff"]] \
        .describe() \
        .reset_index(level=0)
    return df_path_size_describe


# OVERVIEW: HIERARCHY EDGE analysis (comparing INPUT vs OUTPUT)
def produce_overview_hierarchy_edge_comparison(data_manager: DataManager) -> List[NamedTable]:
    # input
    input_data_repo = DataRepository()
    input_data_repo.update(tables=data_manager.load_input_tables())
    input_edges = input_data_repo.get(TABLE_EDGES_HIERARCHY).dataframe
    input_nodes = input_data_repo.get(TABLE_NODES).dataframe
    input_nodes_ids = input_nodes[COLUMN_DEFAULT_ID].tolist()
    input_nodes_connected = produce_named_table_nodes_connected(hierarchy_edges=input_edges) \
        .dataframe.query(expr=f"{COLUMN_DEFAULT_ID} == @input_nodes_ids", inplace=False)
    input_nodes_connected_ids = input_nodes_connected[COLUMN_DEFAULT_ID].tolist()
    input_nodes_dangling = input_nodes.query(
        expr=f"{COLUMN_DEFAULT_ID} != @input_nodes_connected_ids", inplace=False)
    input_child_nodes, input_parent_nodes = _get_leaf_and_parent_nodes(hierarchy_edges=input_edges)

    # output
    output_data_repo = DataRepository()
    output_data_repo.update(tables=data_manager.load_output_tables())
    output_data_repo.update(tables=data_manager.load_intermediate_tables())
    output_edges = output_data_repo.get(TABLE_EDGES_HIERARCHY_POST).dataframe
    output_nodes = output_data_repo.get(TABLE_NODES).dataframe
    output_nodes_connected = output_data_repo.get(TABLE_NODES_CONNECTED).dataframe
    output_nodes_dangling = output_data_repo.get(TABLE_NODES_DANGLING).dataframe
    output_child_nodes, output_parent_nodes = _get_leaf_and_parent_nodes(hierarchy_edges=output_edges)

    # counts
    count_input_nodes = len(input_nodes)
    count_input_nodes_connected = len(input_nodes_connected)
    count_input_nodes_dangling = len(input_nodes_dangling)
    count_input_nodes_child = len(input_child_nodes)
    count_input_nodes_parent = len(input_parent_nodes)

    count_output_nodes = len(output_nodes)
    count_output_nodes_connected = len(output_nodes_connected)
    count_output_nodes_dangling = len(output_nodes_dangling)
    count_output_nodes_child = len(output_child_nodes)
    count_output_nodes_parent = len(output_parent_nodes)

    # metric | IN | OUT | DIFF
    data = [
        # general
        ["Graphs (ontologies with hierarchy)", -1, 1, -1, -1, -1, -1],
        ["Edges", len(input_edges), len(output_edges), abs(len(input_edges) - len(output_edges)),
         -1, -1, -1],
        ["Nodes", count_input_nodes, count_output_nodes, abs(count_input_nodes - count_output_nodes),
         -1, -1, -1],
        _get_input_output_comparison(
            metric="Connected nodes",
            input_subset_count=count_input_nodes_connected, input_total_count=count_input_nodes,
            output_subset_count=count_output_nodes_connected, output_total_count=count_output_nodes,
        ),
        _get_input_output_comparison(
            metric="Dangling nodes",
            input_subset_count=count_input_nodes_dangling, input_total_count=count_input_nodes,
            output_subset_count=count_output_nodes_dangling, output_total_count=count_output_nodes,
        ),
        _get_input_output_comparison(
            metric="Child nodes",
            input_subset_count=count_input_nodes_child, input_total_count=count_input_nodes_connected,
            output_subset_count=count_output_nodes_child, output_total_count=count_output_nodes_connected,
        ),
        _get_input_output_comparison(
            metric="Parent nodes",
            input_subset_count=count_input_nodes_parent, input_total_count=count_input_nodes_connected,
            output_subset_count=count_output_nodes_parent, output_total_count=count_output_nodes_connected,
        )
    ]
    data_df = pd.DataFrame(data, columns=["metric",
                                          "input_count", "output_count", "diff_count",
                                          "input_percentage", "output_percentage", "diff_percentage"])
    tables = [NamedTable("hierarchy_edge_comparison", data_df)]
    tables.extend(
        _get_hierarchy_edge_input_children_count_descriptions(
            input_hierarchy_edges=input_edges,
            output_hierarchy_edges=output_edges
        )
    )
    return tables


def _get_input_output_comparison(metric: str,
                                 input_subset_count: int, input_total_count: int,
                                 output_subset_count: int, output_total_count: int) -> List:
    percentage_input = _get_percentage_of(subset_count=input_subset_count, total_count=input_total_count)
    percentage_output = _get_percentage_of(subset_count=output_subset_count, total_count=output_total_count)
    return [
        metric,
        input_subset_count, output_subset_count, abs(input_subset_count - output_subset_count),
        percentage_input, percentage_output, round(abs(percentage_input - percentage_output), 2),
    ]


def _get_leaf_and_parent_nodes(hierarchy_edges: DataFrame) -> (DataFrame, DataFrame):
    df = hierarchy_edges.copy()
    parent_nodes = df[[COLUMN_TARGET_ID]].drop_duplicates(keep="first", inplace=False) \
        .rename(columns={COLUMN_TARGET_ID: COLUMN_DEFAULT_ID})
    source_nodes = df[[COLUMN_SOURCE_ID]].drop_duplicates(keep="first", inplace=False) \
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_DEFAULT_ID})
    leaf_nodes = pd.concat([source_nodes, parent_nodes, parent_nodes]).drop_duplicates(keep=False)
    return leaf_nodes, parent_nodes


def _get_hierarchy_edge_input_children_count_descriptions(
    input_hierarchy_edges: DataFrame, output_hierarchy_edges: DataFrame
) -> List[NamedTable]:

    # output
    output_edge_child_counts = _get_child_counts(hierarchy_edges=output_hierarchy_edges)
    output_edge_child_counts_description = output_edge_child_counts[['children_count']] \
        .describe() \
        .reset_index(level=0).rename(columns={'children_count': 'output_children_count'})

    # input
    input_edge_child_counts = _get_child_counts(hierarchy_edges=input_hierarchy_edges)
    dfs = output_edge_child_counts_description

    # split by namespace
    nss = sorted(list(set(input_edge_child_counts['namespace_target_id'].tolist())))
    for ns in nss:
        input_for_ns = input_edge_child_counts.query(f"namespace_target_id == '{ns}'")
        input_child_counts_description_for_ns = input_for_ns[['children_count']] \
            .describe() \
            .reset_index(level=0).rename(columns={'children_count': f'input_{ns}_children_count'})
        dfs = pd.merge(
            dfs,
            input_child_counts_description_for_ns,
            on="index",
            how="left"
        )

    return [
        NamedTable("hierarchy_edge_output_children_counts", output_edge_child_counts),
        NamedTable("hierarchy_edge_input_children_counts", input_edge_child_counts),
        NamedTable("hierarchy_edge_children_count_comparison", dfs),
    ]


def _get_child_counts(hierarchy_edges: DataFrame) -> DataFrame:
    df = hierarchy_edges.copy()
    df = df[[COLUMN_TARGET_ID, COLUMN_SOURCE_ID]] \
        .groupby([COLUMN_TARGET_ID]) \
        .agg(children_count=(COLUMN_SOURCE_ID, lambda x: len(set(x))),
             children=(COLUMN_SOURCE_ID, lambda x: set(x))) \
        .reset_index() \
        .sort_values('children_count', ascending=False)
    df = produce_table_with_namespace_column_for_node_ids(table=df)
    return df
