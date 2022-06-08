"""Helper methods to analyse input and output data."""

import itertools
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from pandas import DataFrame
from pandas_profiling import __version__ as pandas_profiling_version

from onto_merger.alignment import hierarchy_utils
from onto_merger.analyser import analysis_utils, plotly_utils
from onto_merger.analyser.constants import (
    ANALYSIS_GENERAL,
    COLUMN_FREQ,
    COLUMN_NAMESPACE_SOURCE_ID,
    COLUMN_NAMESPACE_TARGET_ID,
    GANTT_CHART,
    TABLE_SECTION_SUMMARY,
    TABLE_STATS,
)
from onto_merger.data.constants import (
    COLUMN_COUNT,
    COLUMN_DEFAULT_ID,
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE_ID,
    COLUMN_SOURCE_TO_TARGET,
    COLUMN_TARGET_ID,
    DIRECTORY_DOMAIN,
    DIRECTORY_INPUT,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_OUTPUT,
    DOMAIN_SUFFIX,
    SCHEMA_NODE_ID_LIST_TABLE,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_CONNECTIVITY_STEPS_REPORT,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_DOMAIN,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MAPPINGS,
    TABLE_MAPPINGS_DOMAIN,
    TABLE_NODES,
    TABLE_NODES_CONNECTED,
    TABLE_NODES_CONNECTED_EXC_SEED,
    TABLE_NODES_DANGLING,
    TABLE_NODES_DOMAIN,
    TABLE_NODES_MERGED,
    TABLE_NODES_MERGED_TO_OTHER,
    TABLE_NODES_MERGED_TO_SEED,
    TABLE_NODES_OBSOLETE,
    TABLE_NODES_SEED,
    TABLE_NODES_UNMAPPED,
    TABLE_PIPELINE_STEPS_REPORT,
    TABLE_TYPE_EDGE,
    TABLE_TYPE_MAPPING,
    TABLE_TYPE_MERGE,
    TABLE_TYPE_NODE,
    TABLES_DOMAIN,
    TABLES_EDGE_HIERARCHY,
    TABLES_INPUT,
    TABLES_INTERMEDIATE,
    TABLES_MAPPING,
    TABLES_MERGE,
    TABLES_NODE,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import DataRepository, NamedTable
from onto_merger.report.constants import (
    SECTION_ALIGNMENT,
    SECTION_CONNECTIVITY,
    SECTION_INPUT,
    SECTION_OUTPUT,
    SECTION_OVERVIEW,
)
from onto_merger.version import __version__ as onto_merger_version

COVERED = "covered"
FLOAT_ROUND_TO = 2


# SUMMARY SUBSECTIONS #
def produce_summary_overview(
        data_manager: DataManager, data_repo: DataRepository,
) -> NamedTable:
    """Produce the overview section summary.

    :param data_manager: The data manager instance.
    :param data_repo: The data repository containing the produced tables.
    :return: The summary as a named table.
    """
    config = data_manager.load_alignment_config()
    steps_report = data_repo.get(table_name=TABLE_PIPELINE_STEPS_REPORT).dataframe
    elapsed_time = timedelta(seconds=int(steps_report['elapsed'].sum()))
    summary = [
        {"metric": "Dataset (folder name)",
         "values": f"<code>{data_manager.get_project_folder_path().split('/')[-1]}</code>"},
        {"metric": "Dataset",
         "values": '<a href="../.." target="_blank">Link</a>'},
        {"metric": "Domain", "values": config.base_config.domain_node_type},
        {"metric": "Seed ontology", "values": config.base_config.seed_ontology_name},
        {"metric": "Total runtime", "values": elapsed_time},
        {"metric": "OntoMerger version", "values": f"<code>{onto_merger_version}</code>"},
    ]
    return NamedTable(
        TABLE_SECTION_SUMMARY,
        pd.DataFrame(summary)
    )


def produce_summary_input(data_repo: DataRepository) -> NamedTable:
    """Produce the input section summary.

    :param data_repo: The data repository containing the produced tables.
    :return: The summary as a named table.
    """
    summary = [
        {"metric": "Dataset",
         "values": '<a href="../../input" target="_blank">Link</a>'},
        {"metric": "Number of nodes", "values": len(data_repo.get(table_name=TABLE_NODES).dataframe)},
        {"metric": "Number of obsolete nodes", "values": len(data_repo.get(table_name=TABLE_NODES_OBSOLETE).dataframe)},
        {"metric": "Number of mappings", "values": len(data_repo.get(table_name=TABLE_MAPPINGS).dataframe)},
        {"metric": "Number of hierarchy edges",
         "values": len(data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe)},
    ]
    return NamedTable(
        TABLE_SECTION_SUMMARY,
        pd.DataFrame(summary)
    )


def produce_summary_output(data_repo: DataRepository) -> NamedTable:
    """Produce the output section summary.

    :param data_repo: The data repository containing the produced tables.
    :return: The summary as a named table.
    """""
    nodes_connected = data_repo.get(TABLE_NODES_CONNECTED).dataframe
    nb_unique_nodes = len(data_repo.get(table_name=TABLE_NODES_DOMAIN).dataframe)
    summary = [
        {"metric": "Dataset",
         "values": '<a href="../../output/domain_ontology" target="_blank">Link</a>'},
        {"metric": "Number of unique nodes", "values": nb_unique_nodes},
        {"metric": "Number of merged nodes",
         "values": len(data_repo.get(table_name=TABLE_NODES_MERGED).dataframe)},
        {"metric": "Number of connected nodes (in hierarchy)", "values": len(nodes_connected)},
        {"metric": "Number of dangling nodes (not in hierarchy)",
         "values": f"{len(data_repo.get(TABLE_NODES_DANGLING).dataframe)}"},
        {"metric": "Number of mappings", "values": len(data_repo.get(table_name=TABLE_MAPPINGS_DOMAIN).dataframe)},
        {"metric": "Number of hierarchy edges",
         "values": len(data_repo.get(table_name=TABLE_EDGES_HIERARCHY_DOMAIN).dataframe)},
    ]
    return NamedTable(
        TABLE_SECTION_SUMMARY,
        pd.DataFrame(summary)
    )


def produce_summary_alignment(data_repo: DataRepository) -> NamedTable:
    """Produce the alignment section summary.

    :param data_repo: The data repository containing the produced tables.
    :return: The summary as a named table.
    """""
    steps_report = data_repo.get(table_name=TABLE_ALIGNMENT_STEPS_REPORT).dataframe
    summary = [
        {"metric": "Process runtime",
         "values": _get_runtime_for_main_step(process_name="ALIGNMENT", data_repo=data_repo)},
        {"metric": "Number of steps", "values": len(steps_report)},
        {"metric": "Number of sources", "values": len(set(steps_report['source'].tolist())) - 1},
        {"metric": "Number of mapping type groups used",
         "values": len(set(steps_report['mapping_type_group'].tolist()))},
        {"metric": "Number of input nodes", "values": len(data_repo.get(TABLE_NODES_DANGLING).dataframe)},
        {"metric": "Number of seed nodes", "values": len(data_repo.get(TABLE_NODES_SEED).dataframe)},
        {"metric": "Number of merged nodes (excluding seed)",
         "values": (len(data_repo.get(TABLE_NODES_MERGED).dataframe)
                    - len(data_repo.get(TABLE_NODES_MERGED_TO_SEED).dataframe))},
        {"metric": "Number of unmapped nodes", "values": len(data_repo.get(TABLE_NODES_UNMAPPED).dataframe)},
    ]
    return NamedTable(
        TABLE_SECTION_SUMMARY,
        pd.DataFrame(summary)
    )


def produce_summary_connectivity(data_repo: DataRepository) -> NamedTable:
    """Produce the connectivity section summary.

    :param data_repo: The data repository containing the produced tables.
    :return: The summary as a named table.
    """""
    steps_report = data_repo.get(table_name=TABLE_CONNECTIVITY_STEPS_REPORT).dataframe
    nodes_connected_seed = steps_report['count_connected_nodes'].iloc[0]
    nodes_connected_not_seed = steps_report['count_connected_nodes'].sum() - nodes_connected_seed
    edges_seed = steps_report['count_available_edges'].iloc[0]
    edges_output = len(data_repo.get(table_name=TABLE_EDGES_HIERARCHY_POST).dataframe)
    edges_produced = edges_output - edges_seed
    summary = [
        {"metric": "Process runtime",
         "values": _get_runtime_for_main_step(process_name="CONNECTIVITY", data_repo=data_repo)},
        {"metric": "Number of steps run", "values": len(steps_report)},
        {"metric": "Number of nodes to connect (excluding seed)",
         "values": len(data_repo.get(table_name=TABLE_NODES_UNMAPPED).dataframe)},
        {"metric": "Number of connected nodes (excluding seed)", "values": nodes_connected_not_seed},
        {"metric": "Number of input hierarchy edges",
         "values": len(data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe)},
        {"metric": "Number of seed hierarchy edges", "values": edges_seed},
        {"metric": "Number of produced hierarchy edges", "values": edges_produced},
    ]
    return NamedTable(
        TABLE_SECTION_SUMMARY,
        pd.DataFrame(summary)
    )


def produce_summary_data_tests(data_repo: DataRepository,
                               stats: DataFrame) -> NamedTable:
    """Produce the data testing section summary.

    :param stats: The analysis of the data testing.
    :param data_repo: The data repository containing the produced tables.
    :return: The summary as a named table.
    """""
    summary = [
        {"metric": "Process runtime",
         "values": _get_runtime_for_main_step(process_name="VALIDATION", data_repo=data_repo)},
        {"metric": "Data docs report",
         "values": '<a href="data_docs/local_site/index.html" target="_blank">Link</a>'},
        {"metric": "Number of tables tested", "values": len(stats)},
        {"metric": "Number of data tests run", "values": stats['nb_validations'].sum()},
        {"metric": "Number of failed tests (input data)",
         "values": stats.query(expr=f"directory == '{DIRECTORY_INPUT}'", inplace=False)
         ['nb_failed_validations'].sum()},
        {"metric": "Number of failed tests (intermediate data)",
         "values": stats.query(expr=f"directory == '{DIRECTORY_INTERMEDIATE}'", inplace=False)
         ['nb_failed_validations'].sum()},
        {"metric": "Number of failed tests (output data)",
         "values": stats.query(expr=f"directory == '{DIRECTORY_DOMAIN}'", inplace=False)
         ['nb_failed_validations'].sum()},
        {"metric": "Total failed tests", "values": stats['nb_failed_validations'].sum()},
        {"metric": "Great Expectations package version", "values": f"<code>{stats['ge_version'].iloc[0]}</code>"},
    ]
    return NamedTable(
        TABLE_SECTION_SUMMARY,
        pd.DataFrame(summary)
    )


def produce_summary_data_profiling(data_repo: DataRepository,
                                   data_profiling_stats: DataFrame) -> NamedTable:
    """Produce the data profiling section summary.

    :param data_profiling_stats: The analysis of the data profiling.
    :param data_repo: The data repository containing the produced tables.
    :return: The summary as a named table.
    """""
    summary = [
        {"metric": "Process runtime",
         "values": _get_runtime_for_main_step(process_name="PROFILING", data_repo=data_repo)},
        {"metric": "Data profiling reports (folder)",
         "values": '<a href="data_profile_reports/" target="_blank">Link</a>'},
        {"metric": "Number of tables profiled", "values": len(data_profiling_stats)},
        {"metric": "Number of rows profiled", "values": data_profiling_stats['rows'].sum()},
        {"metric": "Total file size",
         "values": f"{data_profiling_stats['size_float'].sum() / float(1 << 20):,.3f}MB"},
        {"metric": "Pandas profiling version package version",
         "values": f"<code>{pandas_profiling_version}</code>"},
    ]
    return NamedTable(
        TABLE_SECTION_SUMMARY,
        pd.DataFrame(summary)
    )


# SECTION: OVERVIEW
def produce_validation_overview_analyses(
        data_profiling_stats: DataFrame,
        data_test_stats: DataFrame) -> NamedTable:
    """Produce overview section validation summary analysis.

    :param data_profiling_stats: The analysis of the data profiling.
    :param data_test_stats: The analysis of the data testing.
    :return: The summary as a named table.
    """
    df_merged = pd.merge(
        data_profiling_stats[['directory', 'type', 'name', 'rows', 'columns', 'size_float']],
        data_test_stats[['directory', 'type', 'name', 'nb_validations', 'nb_failed_validations']],
        how="left",
        on=['directory', 'name'],
    )
    summary_data = []
    for directory in [DIRECTORY_INPUT, DIRECTORY_INTERMEDIATE, DIRECTORY_OUTPUT]:
        df = df_merged.query(expr=f"directory == '{directory}'", inplace=False)
        nb_validations = df['nb_validations'].sum()
        nb_failed_validations = df['nb_failed_validations'].sum()
        success_ratio = f"{(nb_validations - nb_failed_validations) / nb_validations * 100:.2f}%"
        summary_data.append(
            [directory, df['name'].count(), df['rows'].sum(), f"{(df['size_float'].sum() / float(1 << 20)):,.2f}MB",
             nb_validations, nb_failed_validations, success_ratio, (True if nb_failed_validations == 0 else False)]
        )
    summary_df = pd.DataFrame(summary_data,
                              columns=['directory', 'tables', 'rows', 'file_size',
                                       'nb_validations', 'nb_failed_validations',
                                       'success_ratio', 'success_status'])
    return NamedTable("data_profiling_and_tests_summary", summary_df)


# SECTION: ALIGNMENT, CONNECTIVITY
def produce_step_node_analysis_plot(
        step_report: DataFrame,
        section_dataset_name: str,
        data_manager: DataManager,
        col_count_a: str,
        col_a: str,
        col_count_b: str,
        col_b: str,
        b_start_value: int,
) -> None:
    """Produce alignment or connectivity step analysis plot.

    :param step_report: The report table to be analysed.
    :param section_dataset_name: The section where the plot will be displayed.
    :param data_manager: The data manager instance.
    :param col_count_a:
    :param col_a:
    :param col_count_b:
    :param col_b:
    :param b_start_value:
    :return:
    """
    col_step_counter = "step_counter"
    col_source = "step_counter"
    total_count = 0
    data = []
    for _, row in step_report.iterrows():
        total_count += row[col_count_a]
        data.append([row[col_step_counter], total_count, col_a, f"{row[col_step_counter]} : {row[col_source]}"])
        data.append([row[col_step_counter], (row[col_count_b] - row[col_count_a]), col_b,
                     f"{row[col_step_counter]} : {row[col_source]}"])
    df = pd.DataFrame(data=data, columns=["step", "count", "status", "step_name"])
    df = _add_freq_column(df=df, total_count=b_start_value, column_name_count=COLUMN_COUNT)
    plotly_utils.produce_vertical_bar_chart_stacked(
        analysis_table=df,
        file_path=data_manager.get_analysis_figure_path(
            dataset=section_dataset_name,
            analysed_table_name="step_node_analysis",
            analysis_table_suffix="stacked_bar_chart"
        ),
    )


# SECTION: DATA TESTING
def produce_data_testing_table_stats(
        data_manager: DataManager
) -> Tuple[DataFrame, List[NamedTable]]:
    """Produce the data testing analysis.

    :param data_manager: The data manager instance.
    :return: The merged analysis and one table each for input, intermediate and output data sets.
    """
    validation_analysis = _produce_ge_validation_analysis(data_manager=data_manager)
    ge_validation_report_map = _produce_ge_validation_report_map(
        validation_folder=data_manager.get_ge_data_docs_validations_folder_path(),
    )
    input_df = pd.DataFrame(
        _produce_data_test_stats_for_directory(
            tables=TABLES_INPUT,
            ge_validation_report_map=ge_validation_report_map,
            validation_analysis=validation_analysis,
            directory=DIRECTORY_INPUT,
        )
    )
    intermediate_df = pd.DataFrame(
        _produce_data_test_stats_for_directory(
            tables=TABLES_INTERMEDIATE,
            ge_validation_report_map=ge_validation_report_map,
            validation_analysis=validation_analysis,
            directory=DIRECTORY_INTERMEDIATE,
        )
    )
    output_df = pd.DataFrame(
        _produce_data_test_stats_for_directory(
            tables=TABLES_DOMAIN,
            ge_validation_report_map=ge_validation_report_map,
            validation_analysis=validation_analysis,
            directory="domain",
        )
    )
    return (
        pd.concat([input_df, intermediate_df, output_df]), [
            NamedTable(f"{DIRECTORY_INPUT}_{TABLE_STATS}", input_df),
            NamedTable(f"{DIRECTORY_INTERMEDIATE}_{TABLE_STATS}", intermediate_df),
            NamedTable(f"{DIRECTORY_OUTPUT}_{TABLE_STATS}", output_df),
        ]
    )


def _produce_ge_validation_report_map(validation_folder: str) -> dict:
    return {
        str(path).split("validations/")[-1].split("/")[0].replace("_table", ""): str(path).split("output/report/")[-1]
        for path in Path(validation_folder).rglob('*.html')
    }


def _produce_ge_validation_analysis(data_manager: DataManager, ) -> dict:
    data: List[dict] = []
    for path in Path(data_manager.get_ge_json_validations_folder_path()).rglob('*.json'):
        with open(str(path)) as json_file:
            validation_json = json.load(json_file)
            data.append(
                {
                    "table_name": validation_json['meta']['active_batch_definition']['datasource_name'].replace(
                        "_datasource", ""),
                    "directory_name": validation_json['meta']['active_batch_definition']['data_asset_name'].split("_")[
                        0],
                    "nb_validations": validation_json['statistics']['evaluated_expectations'],
                    "success_percent": validation_json['statistics']['success_percent'],
                    "nb_failed_validations": validation_json['statistics']['unsuccessful_expectations'],
                    "success": validation_json['success'],
                    "ge_version": validation_json['meta']["great_expectations_version"],
                }
            )
    data_dic = {
        f"{item['directory_name']}_{item['table_name']}": item
        for item in data
    }
    return data_dic


def produce_ge_validation_analysis_as_table(data_manager: DataManager, ) -> DataFrame:
    """Produce the data test result aggregation table.

    :param data_manager: The data manager instance.
    :return: The aggregated result table.
    """
    return pd.DataFrame(_produce_ge_validation_analysis(data_manager=data_manager).values())


def _produce_data_test_stats_for_directory(tables: List[str],
                                           directory: str,
                                           ge_validation_report_map: dict,
                                           validation_analysis: dict) -> List[dict]:
    return [
        {
            "directory": directory,
            "type": _get_table_type_for_table_name(table_name=table),
            "name": f'{table.replace("_domain", "")}.csv',
            "report": ge_validation_report_map.get(table),
            "nb_validations": validation_analysis[f"{directory}_{table}"]["nb_validations"],
            "nb_failed_validations": validation_analysis[f"{directory}_{table}"]["nb_failed_validations"],
            "success_percent": validation_analysis[f"{directory}_{table}"]["success_percent"],
            "ge_version": validation_analysis[f"{directory}_{table}"]["ge_version"],
        }
        for table in tables if "steps_report" not in table
    ]


# SECTION: DATA PROFILING
def produce_data_profiling_table_stats(
        data_manager: DataManager
) -> Tuple[DataFrame, List[NamedTable]]:
    """Produce the data profiling analysis.

    :param data_manager: The data manager instance.
    :return: The merged analysis and one table each for input, intermediate and output data sets.
    """
    input_df = pd.DataFrame(
        _produce_data_profiling_stats_for_directory(
            tables=data_manager.load_input_tables(),
            folder_path=data_manager.get_input_folder_path(),
            directory=DIRECTORY_INPUT,
            data_manager=data_manager
        )
    )
    intermediate_df = pd.DataFrame(
        _produce_data_profiling_stats_for_directory(
            tables=data_manager.load_intermediate_tables(),
            folder_path=data_manager.get_intermediate_folder_path(),
            directory=DIRECTORY_INTERMEDIATE,
            data_manager=data_manager
        )
    )
    output_df = pd.DataFrame(
        _produce_data_profiling_stats_for_directory(
            tables=data_manager.load_output_tables(),
            folder_path=data_manager.get_domain_ontology_folder_path(),
            directory=DIRECTORY_OUTPUT,
            data_manager=data_manager
        )
    )
    return (
        pd.concat([input_df, intermediate_df, output_df]), [
            NamedTable(f"{DIRECTORY_INPUT}_{TABLE_STATS}", input_df),
            NamedTable(f"{DIRECTORY_INTERMEDIATE}_{TABLE_STATS}", intermediate_df),
            NamedTable(f"{DIRECTORY_OUTPUT}_{TABLE_STATS}", output_df),
        ]
    )


def _produce_data_profiling_stats_for_directory(tables: List[NamedTable],
                                                folder_path: str,
                                                directory: str,
                                                data_manager: DataManager) -> List[dict]:
    return [
        {
            "directory": directory,
            "type": _get_table_type_for_table_name(table_name=table.name),
            "name": f'{table.name.replace("_domain", "")}.csv',
            "rows": len(table.dataframe),
            "columns": len(list(table.dataframe)),
            "size": _get_file_size_in_mb_for_named_table(
                table_name=table.name,
                folder_path=folder_path
            ),
            "size_float": _get_file_size_for_named_table(
                table_name=table.name,
                folder_path=folder_path
            ),
            "report": data_manager.get_profiled_table_report_path(
                table_name=table.name,
                relative_path=True
            )
        }
        for table in tables if "steps_report" not in table.name
    ]


# NODE ANALYSIS #
def produce_node_analyses(
        node_table: NamedTable, mappings: DataFrame, edges_hierarchy: DataFrame
) -> NamedTable:
    """Produce the node analysis tables (namespace frequency, mapping and hierarchy coverage).

    :param node_table: The node table used for analysis.
    :param mappings: The mapping table used for analysis.
    :param edges_hierarchy: The hierarchy edge table used for analysis.
    :return: The analysis result table.
    """
    node_namespace_distribution_df = _produce_node_namespace_distribution_with_type(
        nodes=node_table.dataframe, metric_name="namespace"
    )
    node_mapping_coverage_df = _produce_node_covered_by_edge_table(nodes=node_table.dataframe,
                                                                   edges=mappings,
                                                                   coverage_column=COVERED)
    node_mapping_coverage_distribution_df = _produce_node_namespace_distribution_with_type(
        nodes=node_mapping_coverage_df, metric_name="mapping_coverage"
    )
    node_edge_coverage_df = _produce_node_covered_by_edge_table(nodes=node_table.dataframe,
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
    node_analysis = _add_freq_column(df=node_analysis, total_count=len(node_table.dataframe),
                                     column_name_count="namespace_count", freq_col_name="namespace_freq")
    # add relative freq
    node_analysis["mapping_coverage_freq"] = node_analysis.apply(
        lambda x: (round(x['mapping_coverage_count'] / x['namespace_count'] * 100, 2))
        if x['namespace_count'] != 0 else 0,
        axis=1
    )
    node_analysis["edge_coverage_freq"] = node_analysis.apply(
        lambda x: (round(x['edge_coverage_count'] / x['namespace_count'] * 100, 2))
        if x['namespace_count'] != 0 else 0,
        axis=1
    )
    return NamedTable(f"{node_table.name.replace(DOMAIN_SUFFIX, '')}_{ANALYSIS_GENERAL}", node_analysis)


def _produce_node_namespace_distribution_with_type(nodes: DataFrame, metric_name: str) -> DataFrame:
    node_namespace_distribution_df = analysis_utils.produce_table_node_namespace_distribution(
        node_table=analysis_utils.produce_table_with_namespace_column_for_node_ids(table=nodes)
    )[["namespace", "count"]]
    df = node_namespace_distribution_df.rename(
        columns={"count": f"{metric_name}_count"}
    )
    return df


def _produce_node_covered_by_edge_table(nodes: DataFrame,
                                        edges: DataFrame,
                                        coverage_column: str) -> DataFrame:
    node_id_list_of_edges = analysis_utils.produce_table_node_ids_from_edge_table(edges=edges)
    node_id_list_of_edges[coverage_column] = True
    nodes_covered = pd.merge(
        nodes[SCHEMA_NODE_ID_LIST_TABLE],
        node_id_list_of_edges,
        how="left",
        on=COLUMN_DEFAULT_ID,
    ).dropna(subset=[coverage_column])
    return nodes_covered[SCHEMA_NODE_ID_LIST_TABLE]


def produce_node_namespace_freq(nodes: DataFrame) -> DataFrame:
    """Produce a node namespace analysis table.

    :param nodes: The nodes to be analysed.
    :return: The analysis result table.
    """
    df = _produce_node_namespace_distribution_with_type(
        nodes=nodes, metric_name="namespace"
    )
    df["namespace_freq"] = df.apply(
        lambda x: (round((x['namespace_count'] / len(nodes) * 100), FLOAT_ROUND_TO)), axis=1
    )

    return df


# NODE STATUS ANALYSIS #
def produce_node_status_analyses(
        data_manager: DataManager, data_repo: DataRepository,
) -> DataFrame:
    """Produce the node status analysis tables.

    :param data_manager: The data manager instance.
    :param data_repo: The data repository containing the produced tables.
    :return: The analysis result table.
    """
    # INPUT
    nodes_input = len(data_repo.get(table_name=TABLE_NODES).dataframe)
    nodes_seed = len(data_repo.get(table_name=TABLE_NODES_SEED).dataframe)
    nodes_not_seed = nodes_input - nodes_seed

    # ALIGNMENT
    nodes_merged_total = len(data_repo.get(table_name=TABLE_NODES_MERGED).dataframe)
    nodes_aligned = nodes_seed + nodes_merged_total
    nodes_merged_to_seed = len(data_repo.get(TABLE_NODES_MERGED_TO_SEED).dataframe)
    nodes_merged_to_not_seed = len(data_repo.get(TABLE_NODES_MERGED_TO_OTHER).dataframe)
    nodes_unmapped = len(data_repo.get(TABLE_NODES_UNMAPPED).dataframe)

    # CONNECTIVITY
    nodes_connected = len(data_repo.get(table_name=TABLE_NODES_CONNECTED).dataframe)
    nodes_dangling = len(data_repo.get(table_name=TABLE_NODES_DANGLING).dataframe)
    nodes_connected_excluding_seed = len(data_repo.get(table_name=TABLE_NODES_CONNECTED_EXC_SEED).dataframe)

    # OUTPUT
    nodes_input_output_diff = nodes_input - (nodes_input - nodes_merged_total)
    nodes_unique = nodes_input - nodes_merged_total

    # INPUT:    | Seed | Others |
    input_data = [
        [SECTION_INPUT, nodes_seed, "Seed"],
        [SECTION_INPUT, nodes_not_seed, "Other input"],
    ]
    _process_node_status_table_and_plot(
        data=input_data,
        total_count=nodes_input,
        section_dataset_name=SECTION_INPUT,
        data_manager=data_manager,
    )

    # ALG:      | Seed | Merged to seed | Merged | Unmapped |
    alignment_data = [
        [SECTION_ALIGNMENT, nodes_seed, "Seed"],
        [SECTION_ALIGNMENT, nodes_merged_to_seed, "Merged to seed"],
        [SECTION_ALIGNMENT, nodes_merged_to_not_seed, "Merged"],
        [SECTION_ALIGNMENT, nodes_unmapped, "Unmapped"],
    ]
    _process_node_status_table_and_plot(
        data=alignment_data,
        total_count=nodes_input,
        section_dataset_name=SECTION_ALIGNMENT,
        data_manager=data_manager,
    )

    # CON:      | Seed | Merged and Connected | Connected | Dangling |
    connectivity_data = [
        [SECTION_CONNECTIVITY, (nodes_seed + nodes_merged_to_seed), "Seed + merged to seed"],
        [SECTION_CONNECTIVITY, nodes_connected_excluding_seed, "Connected"],
        [SECTION_CONNECTIVITY, nodes_dangling, "Dangling"],
    ]
    _process_node_status_table_and_plot(
        data=connectivity_data,
        total_count=nodes_input,
        section_dataset_name=SECTION_CONNECTIVITY,
        data_manager=data_manager,
    )

    # Output
    output_data = [
        [SECTION_OUTPUT, nodes_connected, "Connected"],
        [SECTION_OUTPUT, nodes_dangling, "Dangling"],
        [SECTION_OUTPUT, nodes_input_output_diff, "Diff from Input"],
    ]
    _process_node_status_table_and_plot(
        data=output_data,
        total_count=nodes_input,
        section_dataset_name=SECTION_OUTPUT,
        data_manager=data_manager,
    )

    # Overview
    overview_data = input_data + alignment_data + connectivity_data + output_data
    overview_df = _process_node_status_table_and_plot(
        data=overview_data,
        total_count=nodes_input,
        section_dataset_name=SECTION_OVERVIEW,
        data_manager=data_manager,
        is_one_bar=False
    )

    # overview status
    overview_node_status_table = [
        [SECTION_INPUT, nodes_input, "<b>Input</b>"],
        [SECTION_INPUT, nodes_seed, "<i>Seed</i>"],
        [SECTION_INPUT, nodes_not_seed, "<i>Other</i>"],
        [SECTION_ALIGNMENT, nodes_aligned, "<b>Aligned (Seed + Merged)</b>"],
        [SECTION_ALIGNMENT, nodes_merged_to_seed, "<i>Merged to Seed</i>"],
        [SECTION_ALIGNMENT, nodes_merged_to_not_seed, "<i>Merged to Other</i>"],
        [SECTION_ALIGNMENT, nodes_unmapped, "<b>Unmapped</b>"],
        [SECTION_CONNECTIVITY, (nodes_seed + nodes_merged_to_seed + nodes_connected_excluding_seed),
         "<b>Connected (Seed + Merged to Seed + Connected)</b>"],
        [SECTION_CONNECTIVITY, nodes_connected_excluding_seed, "<i>Connected</i>"],
        [SECTION_CONNECTIVITY, nodes_dangling, "<b>Dangling</b>"],
        [SECTION_OUTPUT, nodes_input_output_diff, "<b>Input & Output Diff</b>"],
        [SECTION_OUTPUT, nodes_unique, "<b>Unique</b>"],
    ]
    _produce_and_save_node_status_table(
        data=overview_node_status_table,
        total_count=nodes_input,
        section_dataset_name=SECTION_OVERVIEW,
        data_manager=data_manager,
    )
    return overview_df


def _produce_and_save_node_status_table(
        data: List[list], total_count: int, section_dataset_name: str,
        data_manager: DataManager,
) -> DataFrame:
    df = pd.DataFrame(data, columns=["category", "count", "status_no_freq"])
    df = _add_ratio_to_node_status_table(
        node_status_table=df, total_count=total_count,
    )
    data_manager.save_analysis_table(
        analysis_table=df,
        dataset=section_dataset_name,
        analysed_table_name="node",
        analysis_table_suffix="status",
    )
    return df


def _process_node_status_table_and_plot(
        data: List[list], total_count: int, section_dataset_name: str,
        data_manager: DataManager, is_one_bar: bool = True,
) -> DataFrame:
    node_status_table = _produce_and_save_node_status_table(
        data=data,
        total_count=total_count,
        section_dataset_name=section_dataset_name,
        data_manager=data_manager,
    )
    plotly_utils.produce_status_stacked_bar_chart(
        analysis_table=node_status_table,
        file_path=data_manager.get_analysis_figure_path(
            dataset=section_dataset_name,
            analysed_table_name="node",
            analysis_table_suffix="status",
        ),
        is_one_bar=is_one_bar,
    )
    return node_status_table


def _add_ratio_to_node_status_table(node_status_table: DataFrame, total_count: int) -> DataFrame:
    node_status_table = _add_freq_column(
        df=node_status_table, total_count=total_count, column_name_count=COLUMN_COUNT, freq_col_name="ratio"
    )
    node_status_table["status"] = node_status_table.apply(
        lambda x: f"{x['status_no_freq']} ({x['ratio']:.1f}%)", axis=1
    )
    return node_status_table


# MAPPING ANALYSIS #
def produce_mapping_analysis_for_type(mappings: DataFrame) -> DataFrame:
    """Produce a mapping type analysis table.

    :param mappings: The mappings to be analysed.
    :return: The analysis result table.
    """
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_RELATION]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             provs=(COLUMN_PROVENANCE, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df = _add_freq_column(df=df, total_count=len(mappings), column_name_count='count')
    return df


def produce_mapping_analysis_for_prov(mappings: DataFrame) -> DataFrame:
    """Produce a mapping provenance analysis table.

    :param mappings: The mappings to be analysed.
    :return: The analysis result table.
    """
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_PROVENANCE]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             relations=(COLUMN_RELATION, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    return df


def produce_mapping_analysis_for_mapped_nss(mappings: DataFrame) -> DataFrame:
    """Produce a mapped namespace type analysis table.

    :param mappings: The mappings to be analysed.
    :return: The analysis result table.
    """
    col_nss_set = 'nss_set'
    df = analysis_utils.produce_table_with_namespace_column_for_node_ids(table=mappings)
    df[col_nss_set] = df.apply(
        lambda x: str({x[analysis_utils.get_namespace_column_name_for_column(COLUMN_SOURCE_ID)],
                       x[analysis_utils.get_namespace_column_name_for_column(COLUMN_TARGET_ID)]}),
        axis=1
    )
    df = df[[col_nss_set, COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]] \
        .groupby([col_nss_set]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             types=(COLUMN_RELATION, lambda x: set(x)),
             provs=(COLUMN_PROVENANCE, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df = _add_freq_column(df=df, total_count=len(mappings), column_name_count='count')
    return df


# EDGES ANALYSIS #
def produce_edges_analysis_for_mapped_or_connected_nss_heatmap(edges: DataFrame,
                                                               prune: bool = False,
                                                               directed_edge: bool = False) -> DataFrame:
    """Produce a edge analysis table.

    :param directed_edge: True for hierarchy, false for symmetric mappings.
    :param prune: If true 0 values are removed to shrink the table and the corresponding chart.
    :param edges: The edges to be analysed.
    :return: The analysis result table.
    """
    cols = [analysis_utils.get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            analysis_utils.get_namespace_column_name_for_column(COLUMN_TARGET_ID)]
    df = analysis_utils.produce_table_with_namespace_column_for_node_ids(table=edges)
    df = df.groupby(cols).agg(count=(COLUMN_SOURCE_ID, 'count')) \
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


def produce_source_to_target_analysis_for_directed_edge(edges: DataFrame) -> DataFrame:
    """Produce a source to target analysis for a directed edge (hierarchy or asymmetric mapping).

    :param edges: The edges to be analysed.
    :return: The analysis result table.
    """
    cols = [analysis_utils.get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
            analysis_utils.get_namespace_column_name_for_column(COLUMN_TARGET_ID)]
    df = analysis_utils.produce_table_with_namespace_column_pair(
        table=analysis_utils.produce_table_with_namespace_column_for_node_ids(table=edges)) \
        .groupby(cols) \
        .agg(count=(COLUMN_SOURCE_ID, COLUMN_COUNT)) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df[COLUMN_FREQ] = df.apply(
        lambda x: (round((x[COLUMN_COUNT] / len(edges) * 100), 3)), axis=1
    )
    return df


# HIERARCHY ANALYSIS #
def produce_hierarchy_edge_path_analysis(hierarchy_edges_paths: DataFrame) -> List[NamedTable]:
    """Produce hierarchy edge path (length) analysis.

    :param hierarchy_edges_paths: The edges to be analysed.
    :return: The analysis result tables.
    """
    # compute path diff
    if len(hierarchy_edges_paths) == 0:
        return []
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
    columns = ["length_original_path", "length_produced_path", "path_diff"]
    df_path_size_describe = df[columns] \
        .describe() \
        .reset_index(level=0)
    return df_path_size_describe


def produce_connectivity_hierarchy_edge_overview_analyses(
        edges_output: DataFrame, data_manager: DataManager,
) -> List[NamedTable]:
    """Produce the domain ontology hierarchy edge analyses.

    :param edges_output: The domain ontology hierarchy edges to be analysed.
    :param data_manager: The data manager instance.
    :return: The analysis result tables.
    """
    seed_ns = data_manager.load_alignment_config().base_config.seed_ontology_name
    edge_analysis_for_mapped_nss = produce_hierarchy_edge_analysis_for_connected_nss(edges=edges_output)
    rows = []
    connected_to_seed_count = 0
    connected_other_count = 0
    print(edge_analysis_for_mapped_nss)
    for _, row in edge_analysis_for_mapped_nss.iterrows():
        if row[COLUMN_NAMESPACE_TARGET_ID] == row[COLUMN_NAMESPACE_SOURCE_ID]:
            if row[COLUMN_NAMESPACE_TARGET_ID] == seed_ns:
                rows.append(["connectivity", "Seed", row["count"]])
            else:
                connected_other_count += row["count"]
        else:
            if row[COLUMN_NAMESPACE_TARGET_ID] == seed_ns:
                connected_to_seed_count += row["count"]
            else:
                connected_other_count += row["count"]
    rows.extend([
        ["connectivity", "Directly to seed", connected_to_seed_count],
        ["connectivity", "Other", connected_other_count],
    ])
    df = pd.DataFrame(rows, columns=["category", "status_no_freq", "count"])
    df = _add_freq_column(df=df, total_count=len(edges_output), column_name_count=COLUMN_COUNT)
    df["status"] = df.apply(
        lambda x: f"{x['status_no_freq']} ({x['freq']}%)", axis=1
    )
    plotly_utils.produce_status_stacked_bar_chart_edge(
        analysis_table=df,
        file_path=data_manager.get_analysis_figure_path(
            dataset="connectivity",
            analysed_table_name="edges_hierarchy",
            analysis_table_suffix="status",
        ),
    )

    #
    output_child_nodes, output_parent_nodes = _get_leaf_and_parent_nodes(hierarchy_edges=edges_output)
    child_parent_df = pd.DataFrame([
        ["Node position", "Child nodes", len(output_child_nodes)],
        ["Node position", "Parent nodes", len(output_parent_nodes)],
    ], columns=["category", "status_no_freq", "count"])
    child_parent_df = _add_freq_column(
        df=child_parent_df, total_count=len(edges_output), column_name_count=COLUMN_COUNT
    )
    child_parent_df["status"] = child_parent_df.apply(
        lambda x: f"{x['status_no_freq']} ({x['freq']}%)", axis=1
    )
    plotly_utils.produce_status_stacked_bar_chart_edge(
        analysis_table=child_parent_df,
        file_path=data_manager.get_analysis_figure_path(
            dataset="connectivity",
            analysed_table_name="edges_hierarchy",
            analysis_table_suffix="child_parent",
        ),
    )
    return [
        NamedTable("status", df),
        NamedTable("child_parent", child_parent_df)
    ]


def produce_hierarchy_edge_analysis_for_connected_nss(edges: DataFrame) -> DataFrame:
    """Produce hierarchy edge connected namespace analysis.

    :param edges: The hierarchy edges to be analysed.
    :return: The analysis result tables.
    """
    df = analysis_utils.produce_table_with_namespace_column_pair(
        table=analysis_utils.produce_table_with_namespace_column_for_node_ids(table=edges)) \
        .groupby([COLUMN_SOURCE_TO_TARGET, COLUMN_NAMESPACE_SOURCE_ID, COLUMN_NAMESPACE_TARGET_ID]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             provs=(COLUMN_PROVENANCE, lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df = _add_freq_column(df=df, total_count=len(edges), column_name_count=COLUMN_COUNT)
    return df


def _get_leaf_and_parent_nodes(hierarchy_edges: DataFrame) -> Tuple[DataFrame, DataFrame]:
    df = hierarchy_edges.copy()
    parent_nodes = df[[COLUMN_TARGET_ID]].drop_duplicates(keep="first", inplace=False) \
        .rename(columns={COLUMN_TARGET_ID: COLUMN_DEFAULT_ID})
    source_nodes = df[[COLUMN_SOURCE_ID]].drop_duplicates(keep="first", inplace=False) \
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_DEFAULT_ID})
    leaf_nodes = pd.concat([source_nodes, parent_nodes, parent_nodes]).drop_duplicates(keep=False)
    return leaf_nodes, parent_nodes


def produce_overview_hierarchy_edge_comparison(
        data_repo: DataRepository
) -> List[NamedTable]:
    """Produce a comaparison of input and output hierarchy edges.

    :param data_repo: The data repository containing the produced tables.
    :return: The analysis result tables.
    """
    # input
    input_edges = analysis_utils.produce_table_with_namespace_column_for_node_ids(
        table=data_repo.get(TABLE_EDGES_HIERARCHY).dataframe)
    input_nodes = data_repo.get(TABLE_NODES).dataframe
    input_nodes_ids = input_nodes[COLUMN_DEFAULT_ID].tolist()
    input_nodes_connected = hierarchy_utils.produce_named_table_nodes_connected(hierarchy_edges=input_edges) \
        .dataframe.query(
        expr=f"{COLUMN_DEFAULT_ID} == @input_nodes_ids", local_dict={"input_nodes_ids": input_nodes_ids}, inplace=False
    )
    input_nodes_connected_ids = input_nodes_connected[COLUMN_DEFAULT_ID].tolist()
    input_nodes_dangling = input_nodes.query(
        expr=f"{COLUMN_DEFAULT_ID} != @input_nodes_connected_ids",
        local_dict={"input_nodes_connected_ids": input_nodes_connected_ids},
        inplace=False
    )
    input_child_nodes, input_parent_nodes = _get_leaf_and_parent_nodes(hierarchy_edges=input_edges)

    # output
    output_edges = data_repo.get(TABLE_EDGES_HIERARCHY_POST).dataframe
    output_nodes = data_repo.get(TABLE_NODES_DOMAIN).dataframe
    output_nodes_connected = data_repo.get(TABLE_NODES_CONNECTED).dataframe
    output_nodes_dangling = data_repo.get(TABLE_NODES_DANGLING).dataframe
    output_child_nodes, output_parent_nodes = _get_leaf_and_parent_nodes(hierarchy_edges=output_edges)

    # counts
    count_input_nodes = len(input_nodes)
    count_input_nodes_connected = len(input_nodes_connected)
    count_input_nodes_dangling = len(input_nodes_dangling)
    count_input_nodes_child = len(input_child_nodes)
    count_input_nodes_parent = len(input_parent_nodes)
    count_input_sources = len(set(input_edges['namespace_target_id'].tolist()))

    count_output_nodes = len(output_nodes)
    count_output_nodes_connected = len(output_nodes_connected)
    count_output_nodes_dangling = len(output_nodes_dangling)
    count_output_nodes_child = len(output_child_nodes)
    count_output_nodes_parent = len(output_parent_nodes)

    # metric | IN | OUT | DIFF
    data = [
        # general
        ["Graphs (ontologies with hierarchy)", count_input_sources, 1, abs(count_input_sources - 1), "", "", ""],
        ["Edges", len(input_edges), len(output_edges), abs(len(input_edges) - len(output_edges)),
         "", "", ""],
        ["Nodes", count_input_nodes, count_output_nodes, abs(count_input_nodes - count_output_nodes),
         "", "", ""],
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
    tables = [NamedTable("general_comparison", data_df)]
    tables.extend(
        _get_hierarchy_edge_input_children_count_descriptions(
            input_hierarchy_edges=input_edges,
            output_hierarchy_edges=output_edges
        )
    )
    return tables


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

    # split by namespace
    nss = sorted(list(set(input_edge_child_counts['namespace_target_id'].tolist())))
    rows = [["output"] + output_edge_child_counts_description['output_children_count'].tolist()]
    for ns in nss:
        input_for_ns = input_edge_child_counts.query(f"namespace_target_id == '{ns}'")
        input_child_counts_description_for_ns = input_for_ns[['children_count']] \
            .describe() \
            .reset_index(level=0)
        rows.append([ns] + input_child_counts_description_for_ns['children_count'].tolist())
    dfs = pd.DataFrame(rows, columns=["dataset"] + output_edge_child_counts_description['index'].tolist())
    return [
        NamedTable("children_counts_output", output_edge_child_counts),
        NamedTable("children_counts_input", input_edge_child_counts),
        NamedTable("children_count_comparison", dfs),
    ]


def _get_child_counts(hierarchy_edges: DataFrame) -> DataFrame:
    df = hierarchy_edges.copy()
    df = df[[COLUMN_TARGET_ID, COLUMN_SOURCE_ID]] \
        .groupby([COLUMN_TARGET_ID]) \
        .agg(children_count=(COLUMN_SOURCE_ID, lambda x: len(set(x))),
             children=(COLUMN_SOURCE_ID, lambda x: set(x))) \
        .reset_index() \
        .sort_values('children_count', ascending=False)
    df = analysis_utils.produce_table_with_namespace_column_for_node_ids(table=df)
    return df


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


# MERGE ANALYSIS #
def produce_merge_analysis_for_merged_nss_for_canonical(merges: DataFrame) -> DataFrame:
    """Produce analysis for the merged namespaces grouped by the canonical namespace.

    :param merges: The merges to be analysed.
    :return: The analysis result table.
    """
    df = analysis_utils.produce_table_with_namespace_column_pair(
        table=analysis_utils.produce_table_with_namespace_column_for_node_ids(table=merges)) \
        .groupby([analysis_utils.get_namespace_column_name_for_column(COLUMN_TARGET_ID)]) \
        .agg(count=(COLUMN_SOURCE_ID, 'count'),
             source_nss=(analysis_utils.get_namespace_column_name_for_column(COLUMN_SOURCE_ID), lambda x: set(x))) \
        .reset_index() \
        .sort_values(COLUMN_COUNT, ascending=False)
    df = _add_freq_column(df=df, total_count=len(merges), column_name_count=COLUMN_COUNT)
    return df


def produce_merge_cluster_analysis(merges_aggregated: DataFrame, data_manager: DataManager) -> List[NamedTable]:
    """Produce the merge cluster size analysis.

    :param merges_aggregated: The merges with canonical node IDs.
    :param data_manager: The data manager instance.
    :return: The analysis result tables
    """
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
        .agg(count=(column_cluster_size, 'count')) \
        .reset_index()
    plotly_utils.produce_vertical_bar_chart_cluster_size_bins(
        analysis_table=df_bins,
        file_path=data_manager.get_analysis_figure_path(
            dataset="alignment",
            analysed_table_name="merges",
            analysis_table_suffix="cluster_size_bins",
        )
    )

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
        NamedTable("merges_clusters", df),
        NamedTable("merges_cluster_size_description", df_cluster_size_describe),
        NamedTable("merges_cluster_size_bin_freq", df_bins),
        NamedTable("merges_many_nss_merged_to_one", df_many_nss_merged_to_one),
        NamedTable("merges_merges_many_nss_merged_to_one_freq", df_nss_analysis),
    ]
    tables.extend(top_ten_merge_clusters_per_ns)

    return tables


# HELPERS: FILE SIZE ANALYSIS #
def _get_file_size_in_mb_for_named_table(table_name: str,
                                         folder_path: str) -> str:
    f_size = _get_file_size_for_named_table(table_name=table_name, folder_path=folder_path)
    return f"{f_size / float(1 << 20):,.3f}MB"


def _get_file_size_for_named_table(table_name: str,
                                   folder_path: str) -> float:
    table_name_for_file_store = table_name.replace(DOMAIN_SUFFIX, "")
    return os.path.getsize(os.path.abspath(f"{folder_path}/{table_name_for_file_store}.csv"))


# HELPERS: ... #
def _get_table_type_for_table_name(table_name: str) -> str:
    if table_name in TABLES_NODE:
        return TABLE_TYPE_NODE
    elif table_name in TABLES_EDGE_HIERARCHY:
        return TABLE_TYPE_EDGE
    elif table_name in TABLES_MAPPING:
        return TABLE_TYPE_MAPPING
    elif table_name in TABLES_MERGE:
        return TABLE_TYPE_MERGE
    return ""


# RUNTIME ANALYSIS #
def produce_runtime_tables(
        table_name: str,
        section_dataset_name: str,
        data_manager: DataManager,
        data_repo: DataRepository,
) -> List[NamedTable]:
    """Produce the runtime table analysis table and figure.

    :param table_name: The runtime table name.
    :param section_dataset_name: The report section.
    :param data_manager: The data manager instance.
    :param data_repo: The data repository containing the produced tables.
    :return: The analysis result tables.
    """
    # table
    runtime_table = _add_elapsed_seconds_column_to_runtime(
        runtime=data_repo.get(table_name=table_name).dataframe
    )
    # plot
    plotly_utils.produce_gantt_chart(
        analysis_table=runtime_table,
        file_path=data_manager.get_analysis_figure_path(
            dataset=section_dataset_name,
            analysed_table_name="pipeline_steps_report",
            analysis_table_suffix=GANTT_CHART
        ),
    )
    return [
        NamedTable("pipeline_steps_report_step_duration", runtime_table[["task", "elapsed_sec"]]),
        _produce_runtime_overview_named_table(runtime_table=runtime_table)
    ]


def _produce_runtime_overview_named_table(runtime_table: DataFrame) -> NamedTable:
    runtime_overview = [
        ("Number of steps", len(runtime_table)),
        ("Total runtime", timedelta(seconds=int(runtime_table['elapsed'].sum()))),
        ("Start", runtime_table["start"].iloc[0]),
        ("End", runtime_table["end"].iloc[len(runtime_table) - 1]),
    ]
    runtime_overview_df = pd.DataFrame(runtime_overview, columns=["metric", "value"])
    return NamedTable("pipeline_steps_report_runtime_overview", runtime_overview_df)


def _get_runtime_for_main_step(
        process_name: str,
        data_repo: DataRepository,
) -> str:
    runtime_df = _add_elapsed_seconds_column_to_runtime(
        runtime=data_repo.get(table_name=TABLE_PIPELINE_STEPS_REPORT).dataframe
    )
    runtime_table_for_process = runtime_df[runtime_df['task'].str.contains(process_name)]
    elapsed = int(runtime_table_for_process['elapsed'].sum())
    return str(timedelta(seconds=elapsed))


def _add_elapsed_seconds_column_to_runtime(runtime: DataFrame) -> DataFrame:
    runtime['elapsed_sec'] = runtime.apply(
        lambda x: f"{x['elapsed']:.2f} sec",
        axis=1
    )
    return runtime


# HELPERS #
def _get_percentage_of(subset_count: int, total_count: int):
    return round((subset_count / total_count * 100), 2)


def _apply_rounding_to_float_columns(df: DataFrame, column_names: List[str]) -> DataFrame:
    for column_name in column_names:
        df[column_name] = df.apply(lambda x: (round(float(x[column_name]), FLOAT_ROUND_TO)), axis=1)
    return df


def _add_freq_column(df: DataFrame, total_count: int, column_name_count: str, freq_col_name: str = "freq") -> DataFrame:
    df[freq_col_name] = df.apply(
        lambda x: (round((x[column_name_count] / total_count * 100), FLOAT_ROUND_TO))
        if ((x[column_name_count] != 0) and (total_count != 0)) else 0,
        axis=1
    )
    return df
