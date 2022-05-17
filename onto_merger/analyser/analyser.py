from typing import Union

import pandas as pd
from pandas import DataFrame

from onto_merger.analyser.analysis_util import produce_table_with_namespace_column_for_node_ids, \
    produce_table_node_namespace_distribution, produce_table_with_namespace_column_pair
from onto_merger.alignment.hierarchy_utils import produce_node_id_table_from_edge_table, \
    get_namespace_column_name_for_column
from onto_merger.data.constants import SCHEMA_NODE_ID_LIST_TABLE, COLUMN_DEFAULT_ID, COLUMN_COUNT, \
    SCHEMA_HIERARCHY_EDGE_TABLE, COLUMN_PROVENANCE, COLUMN_RELATION, COLUMN_SOURCE_ID, COLUMN_TARGET_ID, \
    DIRECTORY_INPUT, COLUMN_SOURCE_TO_TARGET, DIRECTORY_OUTPUT

TEST_PATH = "/Users/kmnb265/Desktop/test_data"
TEST_PATH_DOM = "/Users/kmnb265/Desktop/test_data/output/domain_ontology"
OUTPUT_PATH = f"{TEST_PATH}/output/intermediate/analysis/"
COVERED = "covered"

from onto_merger.report import plot_generator


def load_csv(table_path: str) -> DataFrame:
    df = pd.read_csv(table_path).drop_duplicates(keep="first", ignore_index=True)
    print(f"Loaded table [{table_path.split('/')[-1]}] with {len(df):,d} row(s).")
    return df


def print_df_stats(df, name):
    print("\n\n", name, "\n\t", len(df), "\n\t", list(df))
    print(df.head(10))


def produce_node_namespace_distribution_with_type(nodes: DataFrame, metric_name: str) -> DataFrame:
    node_namespace_distribution_df = produce_table_node_namespace_distribution(
        node_table=produce_table_with_namespace_column_for_node_ids(table=nodes)
    )[["namespace", "count"]]
    df = node_namespace_distribution_df.rename(
        columns={"count": f"{metric_name}_count"}
    )
    return df


def produce_node_covered_by_edge_table(nodes: DataFrame,
                                       edges: DataFrame,
                                       coverage_column: str) -> DataFrame:
    node_id_list_of_edges = produce_node_id_table_from_edge_table(edges=edges)
    node_id_list_of_edges[coverage_column] = True
    nodes_covered = pd.merge(
        nodes[SCHEMA_NODE_ID_LIST_TABLE],
        node_id_list_of_edges,
        how="left",
        on=COLUMN_DEFAULT_ID,
    ).dropna(subset=[coverage_column])
    print(f"covered nodes = {len(nodes_covered):,d} | nodes = {len(nodes):,d} | edges = {len(edges):,d}")
    return nodes_covered[SCHEMA_NODE_ID_LIST_TABLE]


def produce_node_analysis(nodes: DataFrame, mappings: DataFrame, edges_hierarchy: DataFrame) -> DataFrame:
    node_namespace_distribution_df = produce_node_namespace_distribution_with_type(
        nodes=nodes, metric_name="namespace"
    )

    node_mapping_coverage_df = produce_node_covered_by_edge_table(nodes=nodes,
                                                                  edges=mappings,
                                                                  coverage_column=COVERED)
    node_mapping_coverage_distribution_df = produce_node_namespace_distribution_with_type(
        nodes=node_mapping_coverage_df, metric_name="mapping_coverage"
    )

    node_edge_coverage_df = produce_node_covered_by_edge_table(nodes=nodes,
                                                               edges=edges_hierarchy,
                                                               coverage_column=COVERED)
    node_edge_coverage_distribution_df = produce_node_namespace_distribution_with_type(
        nodes=node_edge_coverage_df, metric_name="edge_coverage"
    )

    # merge
    node_analysis = node_namespace_distribution_df
    if len(node_edge_coverage_distribution_df) > 0:
        node_analysis = pd.merge(
            node_analysis,
            node_mapping_coverage_distribution_df,
            how="outer",
            on="namespace",
        )
    else:
        node_analysis["mapping_coverage_count"] = 0
    if len(node_edge_coverage_distribution_df) > 0:
        node_analysis = pd.merge(
            node_analysis,
            node_edge_coverage_distribution_df,
            how="outer",
            on="namespace",
        ).fillna(0, inplace=False)
    else:
        node_analysis["edge_coverage_count"] = 0

    # add freq
    node_analysis["namespace_freq"] = node_analysis.apply(
        lambda x: ((x['namespace_count'] / len(nodes)) * 100), axis=1
    )
    # add relative freq
    node_analysis["mapping_coverage_freq"] = node_analysis.apply(
        lambda x: (x['mapping_coverage_count'] / x['namespace_count'] * 100), axis=1
    )
    node_analysis["edge_coverage_freq"] = node_analysis.apply(
        lambda x: (x['edge_coverage_count'] / x['namespace_count'] * 100), axis=1
    )

    print_df_stats(node_analysis, "node_analysis")

    return node_analysis


def produce_mapping_analysis_for_type(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_RELATION]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             provs=(COLUMN_PROVENANCE, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    print_df_stats(df, "type")
    return df


def produce_mapping_analysis_for_prov(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_PROVENANCE]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             relations=(COLUMN_RELATION, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    print_df_stats(df, "type")
    return df


def produce_mapping_analysis_for_mapped_nss(mappings: DataFrame) -> DataFrame:
    col_nss_set = 'nss_set'
    df = produce_table_with_namespace_column_for_node_ids(table=mappings)
    df[col_nss_set] = df.apply(
        lambda x: str({x[get_namespace_column_name_for_column(COLUMN_SOURCE_ID)],
                       x[get_namespace_column_name_for_column(COLUMN_TARGET_ID)]}),
        axis=1
    )
    df = df[[col_nss_set, COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]] \
        .groupby([col_nss_set]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             types=(COLUMN_RELATION, lambda x: set(x)),
             provs=(COLUMN_PROVENANCE, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    print_df_stats(df, "nss")
    return df


def produce_edges_analysis_for_mapped_or_connected_nss_heatmap(edges: DataFrame,
                                                               prune: bool = False,
                                                               directed_edge: bool = False):
    cols = [get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            get_namespace_column_name_for_column(COLUMN_TARGET_ID)]
    df = produce_table_with_namespace_column_for_node_ids(table=edges)
    df: DataFrame = df.groupby(cols).agg(count=(COLUMN_SOURCE_ID, 'count')) \
        .reset_index().sort_values(COLUMN_COUNT, ascending=False)

    # matrix
    namespaces = sorted(list(set((df[cols[0]].tolist() + df[cols[1]].tolist()))))
    matrix = [[0 for ns in namespaces] for ns in namespaces]
    for _, row in df.iterrows():
        src_ns = row[cols[0]]
        trg_ns = row[cols[1]]
        count = row[COLUMN_COUNT]
        current_value1 = matrix[namespaces.index(src_ns)][namespaces.index(trg_ns)]
        matrix[namespaces.index(src_ns)][namespaces.index(trg_ns)] += current_value1 + count
        if directed_edge is False:
            current_value2 = matrix[namespaces.index(trg_ns)][namespaces.index(src_ns)]
            matrix[namespaces.index(trg_ns)][namespaces.index(src_ns)] += current_value2 + count
    matrix_df = pd.DataFrame(matrix, columns=namespaces, index=namespaces)

    # prune 0s
    if prune is True:
        matrix_df = matrix_df.loc[~(matrix_df == 0).all(axis=1)]
        matrix_df = matrix_df.loc[:, (matrix_df != 0).any(axis=0)]

    return matrix_df


def produce_hierarchy_edge_analysis_for_mapped_nss(edges: DataFrame) -> DataFrame:
    df = produce_table_with_namespace_column_pair(
        table=produce_table_with_namespace_column_for_node_ids(table=edges)) \
        .groupby([COLUMN_SOURCE_TO_TARGET]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             provs=(COLUMN_PROVENANCE, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df["freq"] = df.apply(
        lambda x: ((x[COLUMN_COUNT] / len(edges)) * 100), axis=1
    )
    print_df_stats(df, "hierarchy_edge")
    return df


def produce_merge_analysis_for_merged_nss(merges: DataFrame) -> DataFrame:
    cols = [get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            get_namespace_column_name_for_column(COLUMN_TARGET_ID)]
    df = produce_table_with_namespace_column_pair(
        table=produce_table_with_namespace_column_for_node_ids(table=merges)) \
        .groupby(cols) \
        .agg(count=(COLUMN_SOURCE_ID, 'count')) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df["freq"] = df.apply(
        lambda x: ((x[COLUMN_COUNT] / len(merges)) * 100), axis=1
    )
    print_df_stats(df, "merges")
    return df

def produce_merge_analysis_for_merged_nss_2(merges: DataFrame) -> DataFrame:
    df = produce_table_with_namespace_column_pair(
        table=produce_table_with_namespace_column_for_node_ids(table=merges)) \
        .groupby([get_namespace_column_name_for_column(COLUMN_TARGET_ID)]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             source_nss=(get_namespace_column_name_for_column(COLUMN_SOURCE_ID), lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df["freq"] = df.apply(
        lambda x: ((x[COLUMN_COUNT] / len(merges)) * 100), axis=1
    )
    print_df_stats(df, "merges")
    return df


# PRODUCE & SAVE for ENTITY #
def produce_and_save_node_analysis(nodes: DataFrame,
                                   nodes_obs: Union[None, DataFrame],
                                   mappings: DataFrame,
                                   edges_hierarchy: DataFrame,
                                   dataset: str) -> None:
    produce_node_analysis(nodes=nodes,
                          mappings=mappings,
                          edges_hierarchy=edges_hierarchy) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_nodes.csv", index=False)
    if nodes_obs:
        produce_node_analysis(nodes=nodes_obs,
                              mappings=mappings,
                              edges_hierarchy=edges_hierarchy) \
            .to_csv(f"{OUTPUT_PATH}/{dataset}_nodes_obsolete.csv", index=False)


def produce_and_save_mapping_analysis(mappings: DataFrame, dataset: str) -> None:
    produce_mapping_analysis_for_prov(mappings=mappings) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_mappings_prov.csv", index=False)
    produce_mapping_analysis_for_type(mappings=mappings) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_mappings_type.csv", index=False)
    produce_mapping_analysis_for_mapped_nss(mappings) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_mappings_mapped_nss.csv", index=False)
    produce_edges_analysis_for_mapped_or_connected_nss_heatmap(mappings, prune=False, directed_edge=False) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_mappings_mapped_nss_heatmap.csv")


def produce_and_save_hierarchy_edge_analysis(edges: DataFrame, dataset: str) -> None:
    produce_hierarchy_edge_analysis_for_mapped_nss(edges=edges) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_hierarchy_edge_connected_nss.csv")
    produce_edges_analysis_for_mapped_or_connected_nss_heatmap(edges=edges, prune=False, directed_edge=True) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_hierarchy_connected_nss_heatmap.csv", index=True)
    produce_edges_analysis_for_mapped_or_connected_nss_heatmap(edges=edges, prune=True, directed_edge=True) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_hierarchy_edge_connected_nss_heatmap_pruned.csv", index=True)


def produce_and_save_merge_analysis(merges: DataFrame, dataset: str) -> None:
    produce_merge_analysis_for_merged_nss(merges=merges) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_merges_nss.csv")
    produce_merge_analysis_for_merged_nss_2(merges=merges) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_merges_nss2.csv")


# PRODUCE & SAVE for DATASET #
def analysis_input_dataset():
    # load
    input_nodes = load_csv(f"{TEST_PATH}/input/nodes.csv")
    input_nodes_obs = load_csv(f"{TEST_PATH}/input/nodes_obsolete.csv")[SCHEMA_NODE_ID_LIST_TABLE]
    input_mappings = load_csv(f"{TEST_PATH}/input/mappings.csv")
    input_edges_hierarchy = load_csv(f"{TEST_PATH}/input/edges_hierarchy.csv")

    # analyse and save
    produce_and_save_node_analysis(nodes=input_nodes,
                                   nodes_obs=input_nodes_obs,
                                   mappings=input_mappings,
                                   edges_hierarchy=input_edges_hierarchy,
                                   dataset=DIRECTORY_INPUT)
    produce_and_save_mapping_analysis(mappings=load_csv(f"{TEST_PATH}/input/mappings 2.csv"),
                                      dataset=DIRECTORY_INPUT)
    produce_and_save_hierarchy_edge_analysis(edges=input_edges_hierarchy,
                                             dataset=DIRECTORY_INPUT)


def analysis_output_dataset():
    # load

    output_nodes = load_csv(f"{TEST_PATH_DOM}/nodes.csv")
    output_mappings = load_csv(f"{TEST_PATH_DOM}/mappings.csv")
    output_merges = load_csv(f"{TEST_PATH_DOM}/merges.csv")
    output_edges_hierarchy = load_csv(f"{TEST_PATH_DOM}/edges_hierarchy.csv")

    # analyse and save
    # produce_and_save_node_analysis(nodes=output_nodes,
    #                                nodes_obs=None,
    #                                mappings=output_mappings,
    #                                edges_hierarchy=output_edges_hierarchy,
    #                                dataset=DIRECTORY_OUTPUT)
    # produce_and_save_mapping_analysis(mappings=output_mappings,
    #                                   dataset=DIRECTORY_OUTPUT)
    # produce_and_save_hierarchy_edge_analysis(edges=output_edges_hierarchy,
    #                                          dataset=DIRECTORY_OUTPUT)
    produce_and_save_merge_analysis(merges=output_merges,
                                    dataset=DIRECTORY_OUTPUT)


analysis_output_dataset()
