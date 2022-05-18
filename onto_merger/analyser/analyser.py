"""Analyse input and produced data, pipeline processing, data profiling and data tests.

Produce data and figures are presented in the report.
"""

import os
from typing import Union, List
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import NamedTable, DataRepository
from onto_merger.analyser.analysis_util import produce_table_with_namespace_column_for_node_ids, \
    produce_table_node_namespace_distribution, produce_table_with_namespace_column_pair
from onto_merger.alignment.hierarchy_utils import produce_node_id_table_from_edge_table, \
    get_namespace_column_name_for_column
from onto_merger.data.constants import SCHEMA_NODE_ID_LIST_TABLE, COLUMN_DEFAULT_ID, COLUMN_COUNT, \
    COLUMN_PROVENANCE, COLUMN_RELATION, COLUMN_SOURCE_ID, COLUMN_TARGET_ID, \
    DIRECTORY_INPUT, COLUMN_SOURCE_TO_TARGET, DIRECTORY_OUTPUT, TABLES_NODE, TABLES_EDGE_HIERARCHY, TABLES_MAPPING, \
    TABLE_TYPE_MAPPING, TABLES_MERGE, TABLE_TYPE_NODE, TABLE_TYPE_EDGE, DIRECTORY_INTERMEDIATE, \
    DIRECTORY_DOMAIN_ONTOLOGY, TABLE_NODES_OBSOLETE, TABLE_MAPPINGS, TABLE_EDGES_HIERARCHY, TABLE_NODES
from onto_merger.analyser.constants import TABLE_SECTION_SUMMARY, TABLE_NODE_ANALYSIS, TABLE_STATS

TEST_PATH = "/Users/kmnb265/Desktop/test_data"
TEST_PATH_DOM = "/Users/kmnb265/Desktop/test_data/output/domain_ontology"
TEST_PATH_INTER = "/Users/kmnb265/Desktop/test_data/output/intermediate"
OUTPUT_PATH = f"{TEST_PATH}/output/intermediate/analysis/"
COVERED = "covered"


# HELPERS: todo remove #
def load_csv(table_path: str) -> DataFrame:
    df = pd.read_csv(table_path).drop_duplicates(keep="first", ignore_index=True)
    print(f"Loaded table [{table_path.split('/')[-1]}] with {len(df):,d} row(s).")
    return df


def print_df_stats(df, name):
    print("\n\n", name, "\n\t", len(df), "\n\t", list(df))
    print(df.head(10))


# ANALYSIS #
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


def produce_node_namespace_freq(nodes: DataFrame) -> DataFrame:
    df = produce_node_namespace_distribution_with_type(
        nodes=nodes, metric_name="namespace"
    )
    df["namespace_freq"] = df.apply(
        lambda x: ((x['namespace_count'] / len(nodes)) * 100), axis=1
    )
    return df


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
    matrix = [range(0, len(namespaces)) for ns in namespaces]
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


def produce_merge_analysis_for_merged_nss_for_canonicial(merges: DataFrame) -> DataFrame:
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


# TABLE DATA STATS #
def get_table_type_for_table_name(table_name: str) -> str:
    if table_name in TABLES_NODE:
        return TABLE_TYPE_NODE
    elif table_name in TABLES_EDGE_HIERARCHY:
        return TABLE_TYPE_EDGE
    elif table_name in TABLES_MAPPING:
        return TABLE_TYPE_MAPPING
    elif table_name in TABLES_MERGE:
        return TABLE_TYPE_MAPPING


def get_file_size_in_mb_for_named_table(table_name: str,
                                        folder_path: str):
    f_size = os.path.getsize(os.path.abspath(f"{folder_path}/{table_name}.csv"))
    return f"{f_size / float(1 << 20):,.3f} MB"


def produce_ge_validation_report_map(validation_folder: str):
    return {
        str(path).split("validations/")[-1].split("/")[0].replace("_table", ""): str(path).split("output/")[-1]
        for path in Path(validation_folder).rglob('*.html')
    }


def produce_table_stats_for_directory(tables: List[NamedTable],
                                      folder_path: str,
                                      data_manager: DataManager):
    ge_validation_report_map = produce_ge_validation_report_map(
        validation_folder=analysis_data_manager.get_ge_data_docs_validations_folder_path(),
    )
    return [
        {
            "type": get_table_type_for_table_name(table_name=table.name),
            "name": f"{table.name}.csv",
            "rows": len(table.dataframe),
            "size": get_file_size_in_mb_for_named_table(
                table_name=table.name,
                folder_path=folder_path
            ),
            "data_profiling": data_manager.get_profiled_table_report_path(
                table_name=table.name,
                relative_path=True
            ),
            "data_tests": ge_validation_report_map.get(table.name)
        }
        for table in tables
    ]


def produce_table_stats(data_manager: DataManager) -> None:
    pd.DataFrame(
        produce_table_stats_for_directory(
            tables=data_manager.load_input_tables(),
            folder_path=data_manager.get_input_folder_path(),
            data_manager=data_manager
        )
    ).to_csv(f"{data_manager.get_analysis_folder_path()}/{DIRECTORY_INPUT}_{TABLE_STATS}.csv")
    pd.DataFrame(
        produce_table_stats_for_directory(
            tables=data_manager.load_intermediate_tables(),
            folder_path=data_manager.get_intermediate_folder_path(),
            data_manager=data_manager
        )
    ).to_csv(f"{data_manager.get_analysis_folder_path()}/{DIRECTORY_INTERMEDIATE}_{TABLE_STATS}.csv")
    pd.DataFrame(
        produce_table_stats_for_directory(
            tables=data_manager.load_output_tables(),
            folder_path=data_manager.get_domain_ontology_folder_path(),
            data_manager=data_manager
        )
    ).to_csv(f"{data_manager.get_analysis_folder_path()}/{DIRECTORY_OUTPUT}_{TABLE_STATS}.csv")


# SECTION SUMMARIES #
def produce_and_save_summary_input(data_manager: DataManager, data_repo: DataRepository):
    summary = [
        {"metric": "Number of nodes", "values": len(data_repo.get(table_name=TABLES_NODE).dataframe)},
        {"metric": "Number of obsolete nodes", "values": len(data_repo.get(table_name=TABLE_NODES_OBSOLETE).dataframe)},
        {"metric": "Number of mappings", "values": len(data_repo.get(table_name=TABLE_MAPPINGS).dataframe)},
        {"metric": "Number of hierarchy edges",
         "values": len(data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe)},
    ]
    pd.DataFrame(summary).to_csv(
        f"{data_manager.get_analysis_folder_path()}/{DIRECTORY_INPUT}_{TABLE_SECTION_SUMMARY}.csv")


# PRODUCE & SAVE for ENTITY #
def produce_and_save_node_analysis(nodes: DataFrame,
                                   nodes_obs: Union[None, DataFrame],
                                   mappings: DataFrame,
                                   edges_hierarchy: DataFrame,
                                   dataset: str) -> None:
    produce_node_analysis(nodes=nodes,
                          mappings=mappings,
                          edges_hierarchy=edges_hierarchy) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_{TABLE_NODE_ANALYSIS}.csv", index=False)
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
    produce_merge_analysis_for_merged_nss_for_canonicial(merges=merges) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_merges_nss2.csv")


# PRODUCE & SAVE for DATASET #
def produce_input_dataset_report_data(data_manager: DataManager):
    # load data
    data_repo = DataRepository()
    data_repo.update(tables=data_manager.load_input_tables())

    # analyse and save
    produce_and_save_node_analysis(nodes=data_repo.get(table_name=TABLE_NODES).dataframe,
                                   nodes_obs=data_repo.get(
                                       table_name=TABLE_NODES_OBSOLETE).dataframe[SCHEMA_NODE_ID_LIST_TABLE],
                                   mappings=data_repo.get(table_name=TABLE_MAPPINGS).dataframe,
                                   edges_hierarchy=data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe,
                                   dataset=DIRECTORY_INPUT)
    produce_and_save_mapping_analysis(mappings=data_repo.get(table_name=TABLE_MAPPINGS).dataframe,
                                      dataset=DIRECTORY_INPUT)
    produce_and_save_hierarchy_edge_analysis(edges=data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe,
                                             dataset=DIRECTORY_INPUT)
    produce_and_save_summary_input(data_manager=data_manager, data_repo=data_repo)


def analyse_output_dataset():
    # load
    output_nodes = load_csv(f"{TEST_PATH_DOM}/nodes.csv")
    output_mappings = load_csv(f"{TEST_PATH_DOM}/mappings.csv")
    output_merges = load_csv(f"{TEST_PATH_DOM}/merges.csv")
    output_edges_hierarchy = load_csv(f"{TEST_PATH_DOM}/edges_hierarchy.csv")

    # analyse and save
    produce_and_save_node_analysis(nodes=output_nodes,
                                   nodes_obs=None,
                                   mappings=output_mappings,
                                   edges_hierarchy=output_edges_hierarchy,
                                   dataset=DIRECTORY_OUTPUT)
    produce_and_save_mapping_analysis(mappings=output_mappings,
                                      dataset=DIRECTORY_OUTPUT)
    produce_and_save_hierarchy_edge_analysis(edges=output_edges_hierarchy,
                                             dataset=DIRECTORY_OUTPUT)
    produce_and_save_merge_analysis(merges=output_merges,
                                    dataset=DIRECTORY_OUTPUT)


def analyse_alignment_process_dataset():
    # load
    nodes_merged = load_csv(f"{TEST_PATH_INTER}/nodes_merged.csv")
    nodes_unmapped = load_csv(f"{TEST_PATH_INTER}/nodes_unmapped.csv")

    # analyse and save
    dataset = "alignment"
    produce_node_namespace_freq(nodes=nodes_merged) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_nodes_merged_ns_freq.csv")
    produce_node_namespace_freq(nodes=nodes_unmapped) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_nodes_unmapped_ns_freq.csv")


def analyse_connectivity_process_dataset():
    # load
    nodes_dangling = load_csv(f"{TEST_PATH_INTER}/nodes_dangling.csv")
    nodes_merged = load_csv(f"{TEST_PATH_INTER}/nodes_merged.csv")

    # analyse and save
    dataset = "connectivity"
    produce_node_namespace_freq(nodes=nodes_dangling) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_nodes_dangling_ns_freq.csv")
    produce_node_namespace_freq(nodes=nodes_merged) \
        .to_csv(f"{OUTPUT_PATH}/{dataset}_nodes_merged_ns_freq.csv")


def produce_data_profiling_and_testing_report_data(data_manager: DataManager):
    produce_table_stats(data_manager=data_manager)


project_folder_path = os.path.abspath(TEST_PATH)
analysis_data_manager = DataManager(project_folder_path=project_folder_path,
                                    clear_output_directory=False)
# produce_input_dataset_report_data(data_manager=analysis_data_manager)
produce_data_profiling_and_testing_report_data(data_manager=analysis_data_manager)
