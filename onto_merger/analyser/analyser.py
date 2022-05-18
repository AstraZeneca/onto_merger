"""Analyse input and produced data, pipeline processing, data profiling and data tests.

Produce data and figures are presented in the report.
"""

import os
from pathlib import Path
from typing import Union, List

import pandas as pd
from pandas import DataFrame

from onto_merger.alignment.hierarchy_utils import produce_node_id_table_from_edge_table, \
    get_namespace_column_name_for_column
from onto_merger.analyser.analysis_util import produce_table_with_namespace_column_for_node_ids, \
    produce_table_node_namespace_distribution, produce_table_with_namespace_column_pair
from onto_merger.analyser.constants import TABLE_STATS, \
    ANALYSIS_NODE_NAMESPACE_FREQ, \
    TABLE_SECTION, TABLE_SUMMARY, ANALYSIS_GENERAL, ANALYSIS_PROV, ANALYSIS_TYPE, ANALYSIS_MAPPED_NSS, \
    HEATMAP_MAPPED_NSS, ANALYSIS_CONNECTED_NSS, HEATMAP_CONNECTED_NSS, ANALYSIS_MERGES_NSS, \
    ANALYSIS_MERGES_NSS_FOR_CANONICAL, COLUMN_NAMESPACE_TARGET_ID, COLUMN_NAMESPACE_SOURCE_ID, COLUMN_FREQ, \
    ANALYSIS_CONNECTED_NSS_CHART, GANTT_CHART
from onto_merger.data.constants import SCHEMA_NODE_ID_LIST_TABLE, COLUMN_DEFAULT_ID, COLUMN_COUNT, \
    COLUMN_PROVENANCE, COLUMN_RELATION, COLUMN_SOURCE_ID, COLUMN_TARGET_ID, \
    DIRECTORY_INPUT, COLUMN_SOURCE_TO_TARGET, DIRECTORY_OUTPUT, TABLES_NODE, TABLES_EDGE_HIERARCHY, TABLES_MAPPING, \
    TABLE_TYPE_MAPPING, TABLES_MERGE, TABLE_TYPE_NODE, TABLE_TYPE_EDGE, DIRECTORY_INTERMEDIATE, \
    TABLE_NODES_OBSOLETE, TABLE_MAPPINGS, TABLE_EDGES_HIERARCHY, TABLE_NODES, TABLE_MERGES, \
    TABLE_NODES_MERGED, TABLE_NODES_UNMAPPED, TABLE_NODES_DANGLING, TABLE_NODES_CONNECTED_ONLY, \
    TABLE_ALIGNMENT_STEPS_REPORT, TABLE_CONNECTIVITY_STEPS_REPORT, TABLE_PIPELINE_STEPS_REPORT, COLUMN_NAMESPACE, \
    TABLE_EDGES_HIERARCHY_POST
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import NamedTable, DataRepository
from onto_merger.logger.log import get_logger
from onto_merger.report.data.constants import SECTION_INPUT, SECTION_OUTPUT, SECTION_DATA_TESTS, \
    SECTION_DATA_PROFILING, SECTION_CONNECTIVITY, SECTION_OVERVIEW, SECTION_ALIGNMENT
from onto_merger.analyser import plotly_utils

logger = get_logger(__name__)

COVERED = "covered"


# HELPERS todo
def print_df_stats(df, name):
    print("\n\n", name, "\n\t", len(df), "\n\t", list(df))
    print(df.head(10))


# ANALYSIS #
def _produce_node_namespace_distribution_with_type(nodes: DataFrame, metric_name: str) -> DataFrame:
    node_namespace_distribution_df = produce_table_node_namespace_distribution(
        node_table=produce_table_with_namespace_column_for_node_ids(table=nodes)
    )[["namespace", "count"]]
    df = node_namespace_distribution_df.rename(
        columns={"count": f"{metric_name}_count"}
    )
    return df


def _produce_node_covered_by_edge_table(nodes: DataFrame,
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
    return nodes_covered[SCHEMA_NODE_ID_LIST_TABLE]


def _produce_node_analysis(nodes: DataFrame, mappings: DataFrame, edges_hierarchy: DataFrame) -> DataFrame:
    node_namespace_distribution_df = _produce_node_namespace_distribution_with_type(
        nodes=nodes, metric_name="namespace"
    )

    node_mapping_coverage_df = _produce_node_covered_by_edge_table(nodes=nodes,
                                                                   edges=mappings,
                                                                   coverage_column=COVERED)
    node_mapping_coverage_distribution_df = _produce_node_namespace_distribution_with_type(
        nodes=node_mapping_coverage_df, metric_name="mapping_coverage"
    )

    node_edge_coverage_df = _produce_node_covered_by_edge_table(nodes=nodes,
                                                                edges=edges_hierarchy,
                                                                coverage_column=COVERED)
    node_edge_coverage_distribution_df = _produce_node_namespace_distribution_with_type(
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
    return node_analysis


def _produce_node_namespace_freq(nodes: DataFrame) -> DataFrame:
    df = _produce_node_namespace_distribution_with_type(
        nodes=nodes, metric_name="namespace"
    )
    df["namespace_freq"] = df.apply(
        lambda x: ((x['namespace_count'] / len(nodes)) * 100), axis=1
    )
    return df


def _produce_mapping_analysis_for_type(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_RELATION]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             provs=(COLUMN_PROVENANCE, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    return df


def _produce_mapping_analysis_for_prov(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_PROVENANCE]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             relations=(COLUMN_RELATION, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    return df


def _produce_mapping_analysis_for_mapped_nss(mappings: DataFrame) -> DataFrame:
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
    return df


def _produce_edges_analysis_for_mapped_or_connected_nss_heatmap(edges: DataFrame,
                                                                prune: bool = False,
                                                                directed_edge: bool = False) -> DataFrame:
    cols = [get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            get_namespace_column_name_for_column(COLUMN_TARGET_ID)]
    df = produce_table_with_namespace_column_for_node_ids(table=edges)
    df: DataFrame = df.groupby(cols).agg(count=(COLUMN_SOURCE_ID, 'count')) \
        .reset_index().sort_values(COLUMN_COUNT, ascending=False)

    # matrix
    namespaces = sorted(list(set((df[cols[0]].tolist() + df[cols[1]].tolist()))))
    matrix = [[0 for _ in namespaces] for _ in namespaces]
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


def _produce_hierarchy_edge_analysis_for_mapped_nss(edges: DataFrame) -> DataFrame:
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
    return df


def _produce_source_to_target_analysis_for_directed_edge(edges: DataFrame) -> DataFrame:
    cols = [get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            get_namespace_column_name_for_column(COLUMN_TARGET_ID)]
    df = produce_table_with_namespace_column_pair(
        table=produce_table_with_namespace_column_for_node_ids(table=edges)) \
        .groupby(cols) \
        .agg(count=(COLUMN_SOURCE_ID, COLUMN_COUNT)) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df[COLUMN_FREQ] = df.apply(
        lambda x: ((x[COLUMN_COUNT] / len(edges)) * 100), axis=1
    )
    return df


def _produce_merge_analysis_for_merged_nss_for_canonical(merges: DataFrame) -> DataFrame:
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
    return df


# TABLE DATA STATS #
def _get_table_type_for_table_name(table_name: str) -> str:
    if table_name in TABLES_NODE:
        return TABLE_TYPE_NODE
    elif table_name in TABLES_EDGE_HIERARCHY:
        return TABLE_TYPE_EDGE
    elif table_name in TABLES_MAPPING:
        return TABLE_TYPE_MAPPING
    elif table_name in TABLES_MERGE:
        return TABLE_TYPE_MAPPING


def _get_file_size_in_mb_for_named_table(table_name: str,
                                         folder_path: str) -> str:
    f_size = os.path.getsize(os.path.abspath(f"{folder_path}/{table_name}.csv"))
    return f"{f_size / float(1 << 20):,.3f} MB"


def _produce_ge_validation_report_map(validation_folder: str) -> dict:
    return {
        str(path).split("validations/")[-1].split("/")[0].replace("_table", ""): str(path).split("output/")[-1]
        for path in Path(validation_folder).rglob('*.html')
    }


def _produce_table_stats_for_directory(tables: List[NamedTable],
                                       folder_path: str,
                                       data_manager: DataManager) -> List[dict]:
    ge_validation_report_map = _produce_ge_validation_report_map(
        validation_folder=analysis_data_manager.get_ge_data_docs_validations_folder_path(),
    )
    return [
        {
            "type": _get_table_type_for_table_name(table_name=table.name),
            "name": f"{table.name}.csv",
            "rows": len(table.dataframe),
            "size": _get_file_size_in_mb_for_named_table(
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


def _produce_table_stats(data_manager: DataManager) -> None:
    pd.DataFrame(
        _produce_table_stats_for_directory(
            tables=data_manager.load_input_tables(),
            folder_path=data_manager.get_input_folder_path(),
            data_manager=data_manager
        )
    ).to_csv(f"{data_manager.get_analysis_folder_path()}/{DIRECTORY_INPUT}_{TABLE_STATS}.csv")
    pd.DataFrame(
        _produce_table_stats_for_directory(
            tables=data_manager.load_intermediate_tables(),
            folder_path=data_manager.get_intermediate_folder_path(),
            data_manager=data_manager
        )
    ).to_csv(f"{data_manager.get_analysis_folder_path()}/{DIRECTORY_INTERMEDIATE}_{TABLE_STATS}.csv")
    pd.DataFrame(
        _produce_table_stats_for_directory(
            tables=data_manager.load_output_tables(),
            folder_path=data_manager.get_domain_ontology_folder_path(),
            data_manager=data_manager
        )
    ).to_csv(f"{data_manager.get_analysis_folder_path()}/{DIRECTORY_OUTPUT}_{TABLE_STATS}.csv")


# RUNTIME
def _add_elapsed_seconds_column_to_runtime(runtime: DataFrame) -> DataFrame:
    runtime['elapsed_sec'] = runtime.apply(
        lambda x: f"{x['elapsed']:.2f} sec",
        axis=1
    )
    return runtime


def _produce_and_save_runtime_tables(
        table_name: str,
        section_dataset_name: str,
        data_manager: DataManager,
        data_repo: DataRepository,
) -> None:
    runtime_table = _add_elapsed_seconds_column_to_runtime(
        runtime=data_repo.get(table_name=table_name).dataframe
    )
    # plot
    plotly_utils.produce_gantt_chart(
        analysis_table=runtime_table,
        file_path=data_manager.get_analysis_figure_path(
            dataset=section_dataset_name,
            analysed_table_name=table_name,
            analysis_table_suffix=GANTT_CHART
        ),
        label_replacement={}
    )
    # support table: step duration
    data_manager.save_analysis_table(
        analysis_table=runtime_table[["task", "elapsed_sec"]],
        dataset=section_dataset_name,
        analysed_table_name=table_name,
        analysis_table_suffix="step_duration"
    )
    # support table: runtime overview
    runtime_overview = [
        ("START", runtime_table["start"].iloc[0]),
        ("END", runtime_table["end"].iloc[len(runtime_table) - 1]),
        ("RUNTIME", f"{runtime_table['elapsed'].sum()} seconds"),
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(runtime_overview, columns=["metric", "value"]),
        dataset=section_dataset_name,
        analysed_table_name=table_name,
        analysis_table_suffix="runtime_overview"
    )


# SECTION SUMMARIES #
def _produce_and_save_summary_input(data_manager: DataManager, data_repo: DataRepository) -> None:
    summary = [
        {"metric": "Number of nodes", "values": len(data_repo.get(table_name=TABLE_NODES).dataframe)},
        {"metric": "Number of obsolete nodes", "values": len(data_repo.get(table_name=TABLE_NODES_OBSOLETE).dataframe)},
        {"metric": "Number of mappings", "values": len(data_repo.get(table_name=TABLE_MAPPINGS).dataframe)},
        {"metric": "Number of hierarchy edges",
         "values": len(data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe)},
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(summary),
        dataset=SECTION_INPUT,
        analysed_table_name=TABLE_SECTION,
        analysis_table_suffix=TABLE_SUMMARY
    )


def _produce_and_save_summary_output(data_manager: DataManager, data_repo: DataRepository) -> None:
    summary = [
        {"metric": "Number of nodes", "values": len(data_repo.get(table_name=TABLE_NODES).dataframe)},
        {"metric": "Number of merges", "values": len(data_repo.get(table_name=TABLE_MERGES).dataframe)},
        {"metric": "Number of mappings", "values": len(data_repo.get(table_name=TABLE_MAPPINGS).dataframe)},
        {"metric": "Number of hierarchy edges",
         "values": len(data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe)},
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(summary),
        dataset=SECTION_OUTPUT,
        analysed_table_name=TABLE_SECTION,
        analysis_table_suffix=TABLE_SUMMARY
    )


def _produce_and_save_summary_alignment(data_manager: DataManager, data_repo: DataRepository) -> None:
    summary = [
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(summary),
        dataset=SECTION_ALIGNMENT,
        analysed_table_name=TABLE_SECTION,
        analysis_table_suffix=TABLE_SUMMARY
    )


def _produce_and_save_summary_connectivity(data_manager: DataManager, data_repo: DataRepository) -> None:
    summary = [
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(summary),
        dataset=SECTION_CONNECTIVITY,
        analysed_table_name=TABLE_SECTION,
        analysis_table_suffix=TABLE_SUMMARY
    )


def _produce_and_save_summary_data_tests(data_manager: DataManager) -> None:
    summary = [
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(summary),
        dataset=SECTION_DATA_TESTS,
        analysed_table_name=TABLE_SECTION,
        analysis_table_suffix=TABLE_SUMMARY
    )


def _produce_and_save_summary_data_profiling(data_manager: DataManager) -> None:
    summary = [
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(summary),
        dataset=SECTION_DATA_PROFILING,
        analysed_table_name=TABLE_SECTION,
        analysis_table_suffix=TABLE_SUMMARY
    )


def _produce_and_save_summary_overview(data_manager: DataManager, data_repo: DataRepository) -> None:
    summary = [
    ]
    data_manager.save_analysis_table(
        analysis_table=pd.DataFrame(summary),
        dataset=SECTION_OVERVIEW,
        analysed_table_name=TABLE_SECTION,
        analysis_table_suffix=TABLE_SUMMARY
    )


# PRODUCE & SAVE for ENTITY #
def _produce_and_save_node_analysis(node_tables: List[NamedTable],
                                    mappings: DataFrame,
                                    edges_hierarchy: DataFrame,
                                    dataset: str,
                                    data_manager: DataManager) -> None:
    for table in node_tables:
        analysis_table = _produce_node_analysis(
            nodes=table.dataframe,
            mappings=mappings,
            edges_hierarchy=edges_hierarchy
        )
        data_manager.save_analysis_table(
            analysis_table=analysis_table,
            dataset=dataset,
            analysed_table_name=table.name,
            analysis_table_suffix=ANALYSIS_GENERAL
        )
        plotly_utils.produce_nodes_ns_freq_chart(
            analysis_table=analysis_table,
            file_path=data_manager.get_analysis_figure_path(
                dataset=dataset,
                analysed_table_name=table.name,
                analysis_table_suffix=ANALYSIS_GENERAL
            )
        )


def _produce_and_save_mapping_analysis(mappings: DataFrame,
                                       dataset: str,
                                       data_manager: DataManager) -> None:
    table_type = TABLE_MAPPINGS
    data_manager.save_analysis_table(
        analysis_table=_produce_mapping_analysis_for_prov(mappings=mappings),
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=ANALYSIS_PROV
    )
    data_manager.save_analysis_table(
        analysis_table=_produce_mapping_analysis_for_type(mappings=mappings),
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=ANALYSIS_TYPE
    )
    data_manager.save_analysis_table(
        analysis_table=_produce_mapping_analysis_for_mapped_nss(mappings=mappings),
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=ANALYSIS_MAPPED_NSS
    )
    mapped_nss_heatmap_data = _produce_edges_analysis_for_mapped_or_connected_nss_heatmap(
        edges=mappings,
        prune=False,
        directed_edge=False
    )
    data_manager.save_analysis_table(
        analysis_table=mapped_nss_heatmap_data,
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=HEATMAP_MAPPED_NSS,
        index=True
    )
    plotly_utils.produce_edge_heatmap(
        analysis_table=mapped_nss_heatmap_data,
        file_path=data_manager.get_analysis_figure_path(
            dataset=dataset,
            analysed_table_name=table_type,
            analysis_table_suffix=ANALYSIS_MAPPED_NSS
        )
    )


def _produce_and_save_hierarchy_edge_analysis(edges: DataFrame,
                                              dataset: str,
                                              data_manager: DataManager) -> None:
    table_type = TABLE_EDGES_HIERARCHY
    data_manager.save_analysis_table(
        analysis_table=_produce_hierarchy_edge_analysis_for_mapped_nss(edges=edges),
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=ANALYSIS_CONNECTED_NSS
    )
    connected_nss = _produce_source_to_target_analysis_for_directed_edge(edges=edges)
    data_manager.save_analysis_table(
        analysis_table=connected_nss,
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=ANALYSIS_CONNECTED_NSS_CHART
    )
    plotly_utils.produce_hierarchy_nss_stacked_bar_chart(
        analysis_table=connected_nss,
        file_path=data_manager.get_analysis_figure_path(
            dataset=dataset,
            analysed_table_name=table_type,
            analysis_table_suffix=ANALYSIS_CONNECTED_NSS_CHART
        )
    )
    # connected_nss_heatmap = _produce_edges_analysis_for_mapped_or_connected_nss_heatmap(
    #     edges=edges, prune=False, directed_edge=True
    # )
    # data_manager.save_analysis_table(
    #     analysis_table=connected_nss_heatmap,
    #     dataset=dataset,
    #     analysed_table_name=table_type,
    #     analysis_table_suffix=HEATMAP_CONNECTED_NSS,
    #     index=True
    # )
    # plotly_utils.produce_edge_heatmap(
    #     analysis_table=connected_nss_heatmap,
    #     file_path=data_manager.get_analysis_figure_path(
    #         dataset=dataset,
    #         analysed_table_name=table_type,
    #         analysis_table_suffix=HEATMAP_CONNECTED_NSS
    #     )
    # )


def _produce_and_save_merge_analysis(merges: DataFrame,
                                     dataset: str,
                                     data_manager: DataManager) -> None:
    table_type = TABLE_MERGES
    merged_nss = _produce_source_to_target_analysis_for_directed_edge(edges=merges)
    data_manager.save_analysis_table(
        analysis_table=merged_nss,
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=ANALYSIS_MERGES_NSS
    )
    plotly_utils.produce_merged_nss_stacked_bar_chart(
        analysis_table=merged_nss,
        file_path=data_manager.get_analysis_figure_path(
            dataset=dataset,
            analysed_table_name=table_type,
            analysis_table_suffix=ANALYSIS_MERGES_NSS
        )
    )
    data_manager.save_analysis_table(
        analysis_table=_produce_merge_analysis_for_merged_nss_for_canonical(merges=merges),
        dataset=dataset,
        analysed_table_name=table_type,
        analysis_table_suffix=ANALYSIS_MERGES_NSS_FOR_CANONICAL
    )


# NODE STATUS TABLES & CHARTS #
def _produce_and_node_status_analyses(
        seed_name: str, data_manager: DataManager, data_repo: DataRepository,
) -> None:


    # tables
    nodes = data_repo.get(TABLE_NODES).dataframe
    node_namespace_distribution_df = _produce_node_namespace_distribution_with_type(
        nodes=nodes, metric_name="namespace"
    )
    nodes_connected = produce_node_id_table_from_edge_table(edges=data_repo.get(TABLE_EDGES_HIERARCHY_POST).dataframe)
    nodes_connected_only = data_repo.get(TABLE_NODES_CONNECTED_ONLY).dataframe
    nodes_unmapped = data_repo.get(TABLE_NODES_UNMAPPED).dataframe
    nodes_dangling = data_repo.get(TABLE_NODES_DANGLING).dataframe

    # counts
    input_count = len(nodes)
    seed_node_count = node_namespace_distribution_df \
        .query(expr=f'{COLUMN_NAMESPACE} == "{seed_name}"', inplace=False)['namespace_count'].iloc[0]
    non_seed_count = input_count - seed_node_count
    unmapped_count = len(nodes_unmapped)
    connected_count = len(nodes_dangling)
    connected_only_count = len(nodes_connected_only)
    dangling_count = len(nodes_dangling)
    connected_and_merged_count = input_count - (seed_node_count + dangling_count + connected_only_count)
    merge_analysis_df = _produce_merge_analysis_for_merged_nss_for_canonical(
        merges=data_repo.get(TABLE_MERGES).dataframe
    )
    merged_to_seed_count = merge_analysis_df\
        .query(expr=f'{COLUMN_NAMESPACE_TARGET_ID} == "{seed_name}"', inplace=False)['count'].sum()
    merged_not_seed_count = input_count - (seed_node_count + merged_to_seed_count + unmapped_count)

    # INPUT:    | Seed | Others |
    input_data = [
        [SECTION_INPUT, seed_node_count, "Seed"],
        [SECTION_INPUT, non_seed_count, "Other"],
    ]
    _process_node_status_table(
        data=input_data,
        total_count=input_count,
        section_dataset_name=SECTION_INPUT,
        data_manager=data_manager,
    )

    # ALG:      | Seed | Merged to seed | Merged | Unmapped |
    alignment_data = [
        [SECTION_ALIGNMENT, seed_node_count, "Seed"],
        [SECTION_ALIGNMENT, merged_to_seed_count, "Merged to seed"],
        [SECTION_ALIGNMENT, merged_not_seed_count, "Merged"],
        [SECTION_ALIGNMENT, unmapped_count, "Unmapped"],
    ]
    _process_node_status_table(
        data=alignment_data,
        total_count=input_count,
        section_dataset_name=SECTION_ALIGNMENT,
        data_manager=data_manager,
    )

    # CON:      | Seed | Merged and Connected | Connected | Dangling |
    connectivity_data = [
        [SECTION_CONNECTIVITY, seed_node_count, "Seed"],
        [SECTION_CONNECTIVITY, connected_and_merged_count, "Connected and Merged"],
        [SECTION_CONNECTIVITY, connected_only_count, "Connected"],
        [SECTION_CONNECTIVITY, dangling_count, "Dangling"],
    ]
    _process_node_status_table(
        data=connectivity_data,
        total_count=input_count,
        section_dataset_name=SECTION_CONNECTIVITY,
        data_manager=data_manager,
    )

    # Overview
    overview_data = input_data + alignment_data + connectivity_data
    _process_node_status_table(
        data=overview_data,
        total_count=input_count,
        section_dataset_name=SECTION_OVERVIEW,
        data_manager=data_manager,
        showlegend=True
    )


def _process_node_status_table(
        data: List[list], total_count: int, section_dataset_name: str,
        data_manager: DataManager, showlegend: bool = False,
) -> None:
    node_status_table = pd.DataFrame(data, columns=["category", "count", "status_no_freq"])
    node_status_table = _add_ratio_to_node_status_table(
        node_status_table=node_status_table, total_count=total_count,
    )
    data_manager.save_analysis_table(
        analysis_table=node_status_table,
        dataset=section_dataset_name,
        analysed_table_name="node",
        analysis_table_suffix="status",
    )
    plotly_utils.produce_node_status_stacked_bar_chart(
        analysis_table=node_status_table,
        file_path=data_manager.get_analysis_figure_path(
            dataset=section_dataset_name,
            analysed_table_name="node",
            analysis_table_suffix="status",
        ),
        showlegend=showlegend,
    )


def _add_ratio_to_node_status_table(node_status_table: DataFrame, total_count: int) -> DataFrame:
    node_status_table["ratio"] = node_status_table.apply(
        lambda x: x['count'] / total_count * 100, axis=1
    )
    node_status_table["status"] = node_status_table.apply(
        lambda x: f"{x['status_no_freq']} ({x['ratio']:.1f}%)", axis=1
    )
    return node_status_table


# PRODUCE & SAVE for DATASET #
def _produce_input_dataset_analysis(data_manager: DataManager) -> None:
    # load data
    data_repo = DataRepository()
    data_repo.update(tables=data_manager.load_input_tables())

    # analyse and save
    section_dataset_name = SECTION_INPUT
    logger.info(f"Producing report section '{section_dataset_name}' analysis...")
    _produce_and_save_summary_input(data_manager=data_manager, data_repo=data_repo)
    _produce_and_save_node_analysis(
        node_tables=[
            data_repo.get(table_name=TABLE_NODES), data_repo.get(table_name=TABLE_NODES_OBSOLETE)
        ],
        mappings=data_repo.get(table_name=TABLE_MAPPINGS).dataframe,
        edges_hierarchy=data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe,
        dataset=section_dataset_name,
        data_manager=data_manager
    )
    _produce_and_save_mapping_analysis(
        mappings=data_repo.get(table_name=TABLE_MAPPINGS).dataframe,
        dataset=section_dataset_name,
        data_manager=data_manager
    )
    _produce_and_save_hierarchy_edge_analysis(
        edges=data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe,
        dataset=section_dataset_name,
        data_manager=data_manager
    )


def _produce_output_dataset_analysis(data_manager: DataManager) -> None:
    # load data
    data_repo = DataRepository()
    data_repo.update(tables=data_manager.load_output_tables())

    # analyse and save
    section_dataset_name = SECTION_OUTPUT
    logger.info(f"Producing report section '{section_dataset_name}' analysis...")
    _produce_and_save_summary_output(data_manager=data_manager, data_repo=data_repo)
    _produce_and_save_node_analysis(
        node_tables=[
            data_repo.get(table_name=TABLE_NODES)
        ],
        mappings=data_repo.get(table_name=TABLE_MAPPINGS).dataframe,
        edges_hierarchy=data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe,
        dataset=section_dataset_name,
        data_manager=data_manager
    )
    _produce_and_save_mapping_analysis(
        mappings=data_repo.get(table_name=TABLE_MAPPINGS).dataframe,
        dataset=section_dataset_name,
        data_manager=data_manager
    )
    _produce_and_save_hierarchy_edge_analysis(
        edges=data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe,
        dataset=section_dataset_name,
        data_manager=data_manager
    )
    _produce_and_save_merge_analysis(
        merges=data_repo.get(table_name=TABLE_MERGES).dataframe,
        dataset=section_dataset_name,
        data_manager=data_manager
    )


def _produce_alignment_process_analysis(data_manager: DataManager) -> None:
    # load data
    data_repo = DataRepository()
    data_repo.update(tables=data_manager.load_intermediate_tables())

    # analyse and save
    section_dataset_name = SECTION_ALIGNMENT
    logger.info(f"Producing report section '{section_dataset_name}' analysis...")
    _produce_and_save_summary_alignment(data_manager=data_manager, data_repo=data_repo)
    data_manager.save_analysis_table(
        analysis_table=_produce_node_namespace_freq(nodes=data_repo.get(table_name=TABLE_NODES_MERGED).dataframe),
        dataset=section_dataset_name,
        analysed_table_name=TABLE_NODES_MERGED,
        analysis_table_suffix=ANALYSIS_NODE_NAMESPACE_FREQ
    )
    data_manager.save_analysis_table(
        analysis_table=_produce_node_namespace_freq(nodes=data_repo.get(table_name=TABLE_NODES_UNMAPPED).dataframe),
        dataset=section_dataset_name,
        analysed_table_name=TABLE_NODES_UNMAPPED,
        analysis_table_suffix=ANALYSIS_NODE_NAMESPACE_FREQ
    )
    _produce_and_save_runtime_tables(
        table_name=TABLE_ALIGNMENT_STEPS_REPORT,
        section_dataset_name=section_dataset_name,
        data_manager=data_manager,
        data_repo=data_repo,
    )
    # todo merges
    # todo merges aggregated


def _produce_connectivity_process_analysis(data_manager: DataManager) -> None:
    # load data
    data_repo = DataRepository()
    data_repo.update(tables=data_manager.load_intermediate_tables())

    # analyse and save
    section_dataset_name = SECTION_CONNECTIVITY
    logger.info(f"Producing report section '{section_dataset_name}' analysis...")
    _produce_and_save_summary_connectivity(data_manager=data_manager, data_repo=data_repo)
    # todo
    # data_manager.save_analysis_table(
    #     analysis_table=_produce_node_namespace_freq(nodes=data_repo.get(table_name=TABLE_NODES_CONNECTED).dataframe),
    #     dataset=dataset,
    #     analysed_table_name=TABLE_NODES_CONNECTED,
    #     analysis_table_suffix=ANALYSIS_NODE_NAMESPACE_FREQ
    # )
    data_manager.save_analysis_table(
        analysis_table=_produce_node_namespace_freq(
            nodes=data_repo.get(table_name=TABLE_NODES_CONNECTED_ONLY).dataframe),
        dataset=section_dataset_name,
        analysed_table_name=TABLE_NODES_CONNECTED_ONLY,
        analysis_table_suffix=ANALYSIS_NODE_NAMESPACE_FREQ
    )
    data_manager.save_analysis_table(
        analysis_table=_produce_node_namespace_freq(
            nodes=data_repo.get(table_name=TABLE_NODES_DANGLING).dataframe),
        dataset=section_dataset_name,
        analysed_table_name=TABLE_NODES_DANGLING,
        analysis_table_suffix=ANALYSIS_NODE_NAMESPACE_FREQ
    )
    _produce_and_save_runtime_tables(
        table_name=TABLE_CONNECTIVITY_STEPS_REPORT,
        section_dataset_name=section_dataset_name,
        data_manager=data_manager,
        data_repo=data_repo,
    )


def _produce_data_profiling_and_testing_analysis(data_manager: DataManager) -> None:
    logger.info(f"Producing report section '{SECTION_DATA_PROFILING}' and '{SECTION_DATA_TESTS}' analysis...")
    _produce_table_stats(data_manager=data_manager)
    _produce_and_save_summary_data_profiling(data_manager=data_manager)
    _produce_and_save_summary_data_tests(data_manager=data_manager)


def _produce_overview_analysis(data_manager: DataManager) -> None:
    # load data
    data_repo = DataRepository()
    data_repo.update(tables=data_manager.load_input_tables())
    data_repo.update(tables=data_manager.load_output_tables())
    data_repo.update(tables=data_manager.load_intermediate_tables())

    # analyse and save
    section_dataset_name = SECTION_OVERVIEW
    logger.info(f"Producing report section '{section_dataset_name}' analysis...")
    _produce_and_save_summary_overview(data_manager=data_manager, data_repo=data_repo)
    _produce_and_node_status_analyses(seed_name="MONDO", data_manager=data_manager, data_repo=data_repo)
    _produce_and_save_runtime_tables(
        table_name=TABLE_PIPELINE_STEPS_REPORT,
        section_dataset_name=section_dataset_name,
        data_manager=data_manager,
        data_repo=data_repo,
    )


# MAIN #
def produce_report_data(data_manager: DataManager) -> None:
    logger.info(f"Started producing report analysis...")
    # _produce_input_dataset_analysis(data_manager=data_manager)
    # _produce_output_dataset_analysis(data_manager=data_manager)
    # _produce_alignment_process_analysis(data_manager=data_manager)
    # _produce_connectivity_process_analysis(data_manager=data_manager)
    # _produce_data_profiling_and_testing_analysis(data_manager=data_manager)
    _produce_overview_analysis(data_manager=data_manager)
    logger.info(f"Finished producing report analysis.")


# todo
project_folder_path = os.path.abspath("/Users/kmnb265/Documents/GitHub/onto_merger/tests/test_data")
analysis_data_manager = DataManager(project_folder_path=project_folder_path,
                                    clear_output_directory=False)
produce_report_data(data_manager=analysis_data_manager)
