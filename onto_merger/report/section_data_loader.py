"""Loads data for report sections."""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

from onto_merger.analyser.constants import TABLE_SECTION_SUMMARY, TABLE_STATS
from onto_merger.data.constants import (
    DIRECTORY_INPUT,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_OUTPUT,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.logger.log import get_logger
from onto_merger.report.constants import (
    SECTION_ALIGNMENT,
    SECTION_CONNECTIVITY,
    SECTION_DATA_PROFILING,
    SECTION_DATA_TESTS,
    SECTION_INPUT,
    SECTION_OUTPUT,
    SECTION_OVERVIEW,
)
from onto_merger.version import __version__ as onto_merger_version

logger = get_logger(__name__)


DATASET = "dataset"
LINK_TITLE = "link_title"
LOGO = "logo"
METRIC = "metric"
ROWS = "rows"
SECTION_NAME = "section_name"
SUBSECTIONS = "subsections"
TABLE_ANALYSIS = "table_analysis"
TABLE_DESCRIPTION = "table_description"
TEMPLATE = "template"
TITLE = "title"
UNIQUE_ID = "unique_id"


# MAIN #
def load_report_data(data_manager: DataManager) -> dict:
    """Load the HTML report data.

    :param data_manager: The data manager used for loading analysis files.
    :return: The loaded data as a dictionary.
    """
    return {
        "date": f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
        "version": onto_merger_version,
        TITLE: "Report",
        "overview_data": _load_overview_section_data(data_manager=data_manager),
        "input_data": _load_input_section_data(data_manager=data_manager),
        "output_data": _load_output_section_data(data_manager=data_manager),
        "alignment_data": _load_alignment_section_data(data_manager=data_manager),
        "connectivity_data": _load_connectivity_section_data(data_manager=data_manager),
        "data_profiling": _load_data_profiling_section_data(data_manager=data_manager),
        "data_tests": _load_data_testing_section_data(data_manager=data_manager),
    }


# SECTIONS #
def _produce_section(title: str, section_name: str, subsections: List[dict], data_manager: DataManager) -> dict:
    return {
        TITLE: title,
        LINK_TITLE: section_name,
        LOGO: _get_section_icon_file_name(section_name=section_name),
        SUBSECTIONS:
            [_produce_section_summary_subsection(section_name=section_name, data_manager=data_manager)] + subsections
    }


def _load_overview_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_OVERVIEW
    return _produce_section(
        title="Overview",
        section_name=section_name,
        subsections=[
            _produce_runtime_info_subsection(section_name=section_name, data_manager=data_manager),
            _produce_overview_nodes_subsection(section_name=section_name, data_manager=data_manager),
            _produce_overview_edges_subsection(section_name=section_name, data_manager=data_manager),
            _produce_overview_config_subsection(data_manager=data_manager),
            _produce_overview_validation_subsection(section_name=section_name, data_manager=data_manager),
            _produce_overview_project_organisation_subsection(),
            _produce_overview_attributions_subsection(),
        ],
        data_manager=data_manager
    )


def _load_input_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_INPUT
    return _produce_section(
        title="Input",
        section_name=section_name,
        subsections=[
            _produce_nodes_subsection(section_name=section_name, data_manager=data_manager, is_obsolete=False),
            _produce_nodes_subsection(section_name=section_name, data_manager=data_manager, is_obsolete=True),
            _produce_mappings_subsection(section_name=section_name, data_manager=data_manager),
            _produce_edges_hierarchy_subsection(section_name=section_name, data_manager=data_manager),
        ],
        data_manager=data_manager
    )


def _load_alignment_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_ALIGNMENT
    return _produce_section(
        title="Alignment",
        section_name=section_name,
        subsections=[
            _produce_runtime_info_subsection(section_name=section_name, data_manager=data_manager),
            _produce_alignment_detail_subsection(section_name=section_name, data_manager=data_manager),
            _produce_alignment_node_subsection(section_name=section_name, data_manager=data_manager),
            _produce_merges_subsection_main(section_name=section_name, data_manager=data_manager),

        ],
        data_manager=data_manager
    )


def _load_connectivity_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_CONNECTIVITY
    return _produce_section(
        title="Connectivity",
        section_name=section_name,
        subsections=[
            _produce_runtime_info_subsection(section_name=section_name, data_manager=data_manager),
            _produce_connectivity_detail_subsection(section_name=section_name, data_manager=data_manager),
            _produce_connectivity_node_subsection(section_name=section_name, data_manager=data_manager),
            _produce_connectivity_edge_subsection(section_name=section_name, data_manager=data_manager),
        ],
        data_manager=data_manager
    )


def _load_output_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_OUTPUT
    return _produce_section(
        title="Output",
        section_name=section_name,
        subsections=[
            _produce_nodes_subsection(section_name=section_name, data_manager=data_manager, is_obsolete=False),
            _produce_mappings_subsection(section_name=section_name, data_manager=data_manager),
            _produce_edges_hierarchy_subsection(section_name=section_name, data_manager=data_manager),
        ],
        data_manager=data_manager
    )


def _load_data_profiling_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_DATA_PROFILING
    return _produce_section(
        title="Data Profiling",
        section_name=section_name,
        subsections=_produce_data_file_subsections(
            section_name=section_name,
            data_manager=data_manager
        ),
        data_manager=data_manager
    )


def _load_data_testing_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_DATA_TESTS
    return _produce_section(
        title="Data Tests",
        section_name=section_name,
        subsections=_produce_data_file_subsections(
            section_name=section_name,
            data_manager=data_manager
        ),
        data_manager=data_manager
    )


# GENERIC SUBSECTION #
def _produce_section_summary_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        TITLE: "Summary",
        LINK_TITLE: section_name,
        DATASET: {
            "summary_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=TABLE_SECTION_SUMMARY
            ),
        },
        TEMPLATE: "subsection_content/section-summary.html"
    }


def _produce_runtime_info_subsection(section_name: str, data_manager: DataManager) -> dict:
    section_data = {
        TITLE: "Processing",
        LINK_TITLE: "pipeline",
        DATASET: {
            "gantt_img": f"images/{section_name}_pipeline_steps_report_gantt_chart.{data_manager.config.image_format}",
            "runtime_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="pipeline_steps_report_step_duration",
                rename_columns={"task": "metric", "elapsed_sec": "values"},
            ),
            "runtime_summary_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="pipeline_steps_report_runtime_overview",
                rename_columns={"value": "values"},
            ),
            UNIQUE_ID: _get_unique_id_for_description_table(
                section_name=section_name,
                table_name=f"{section_name}_pipeline_steps"
            ),
        },
        TEMPLATE: "subsection_content/subsection-runtime.html"
    }
    return section_data


# SUBSECTIONS #
def _produce_nodes_subsection(section_name: str, data_manager: DataManager, is_obsolete: bool) -> dict:
    table_name = "nodes_general_analysis" if is_obsolete is False else "nodes_obsolete_general_analysis"
    return {
        TITLE: "Nodes" if is_obsolete is False else "Nodes (Obsolete)",
        LINK_TITLE: "nodes" if is_obsolete is False else "nodes_obsolete",
        DATASET: {
            "node_status_fig_path": _get_figure_path(
                section_name=section_name,
                table_name="node_status",
                image_format=data_manager.config.image_format
            ),
            "node_status_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="node_status"
            ),
            TABLE_ANALYSIS: data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            TABLE_DESCRIPTION: _load_table_description_data(table_name="nodes_general_analysis"),
            UNIQUE_ID: _get_unique_id_for_description_table(section_name=section_name,
                                                            table_name=table_name),
            "is_obsolete": is_obsolete,
        },
        TEMPLATE: "subsection_content/dataset-nodes.html"
    }


def _produce_mappings_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "mappings_mapped_nss_analysis"
    return {
        TITLE: "Mappings",
        LINK_TITLE: "mappings",
        DATASET: {
            "analysed_entity": "Mappings",
            "analysis_table_template": "data_content/table_mapping_analysis.html",
            "heatmap_fig_path": _get_figure_path(
                section_name=section_name,
                table_name=table_name,
                image_format=data_manager.config.image_format
            ),
            "heatmap_fig_alt_text": "...",
            "types_fig_path": _get_figure_path(
                section_name=section_name,
                table_name="mappings_type_analysis",
                image_format=data_manager.config.image_format
            ),
            "types_fig_alt_text": "...",
            TABLE_ANALYSIS: data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            TABLE_DESCRIPTION: _load_table_description_data(table_name="mappings_mapped_nss_analysis"),
            UNIQUE_ID: _get_unique_id_for_description_table(section_name=section_name,
                                                            table_name=table_name),
        },
        TEMPLATE: "subsection_content/mapping-analysis.html"
    }


def _produce_edges_hierarchy_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "edges_hierarchy_connected_nss_analysis"
    return {
        TITLE: "Hierarchy edges",
        LINK_TITLE: "edges_hierarchy",
        DATASET: {
            "analysed_entity": "Hierarchy edges",
            "analysis_table_template": "data_content/table_edge_analysis.html",
            "analysis_fig_path": _get_figure_path(section_name=section_name,
                                                  table_name=f"{table_name}_chart",
                                                  image_format=data_manager.config.image_format),
            "analysis_fig_alt_text": "...",
            TABLE_ANALYSIS: data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            TABLE_DESCRIPTION: _load_table_description_data(table_name=table_name),
            UNIQUE_ID: _get_unique_id_for_description_table(section_name=section_name,
                                                            table_name=table_name),
        },
        TEMPLATE: "subsection_content/edges-analysis.html",
    }


# SECTION: OVERVIEW #
def _produce_overview_config_subsection(data_manager: DataManager) -> dict:
    project_name = str(data_manager.get_project_folder_path().split("/")[-1])
    p_name_max = 25
    return {
        TITLE: "Configuration", LINK_TITLE: "configuration",
        DATASET: {
            "dataset_id": f"{project_name[0:p_name_max] if len(project_name) > p_name_max + 1 else project_name}"
                          + f"{'...' if len(project_name) > p_name_max else '' }",
            "config_json": json.dumps(data_manager.load_alignment_config().as_dict, indent=4),
        },
        TEMPLATE: "subsection_content/overview-configuration.html"
    }


def _produce_overview_validation_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        TITLE: "Profiling & Validation",
        LINK_TITLE: "profiling_and_validation",
        DATASET: {
            ROWS: data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="data_profiling_and_tests_summary",
            ),
        },
        TEMPLATE: "data_content/table_validation_summary.html"
    }


def _produce_overview_nodes_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        TITLE: "Node analysis",
        LINK_TITLE: "nodes",
        DATASET: {
            "node_status_fig_path": _get_figure_path(
                section_name=section_name,
                table_name="node_status",
                image_format=data_manager.config.image_format
            ),
            "node_status_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="node_status"
            ),
        },
        TEMPLATE: "subsection_content/overview-node-summary.html"
    }


def _produce_overview_edges_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        TITLE: "Hierarchy analysis",
        LINK_TITLE: "hierarchy_analysis",
        DATASET: {
            "table_general_comparison": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="hierarchy_edge_general_comparison"
            ),
            "table_children_count_comparison": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="hierarchy_edge_children_count_comparison"
            ),
        },
        TEMPLATE: "subsection_content/overview-edge-summary.html"
    }


def _produce_overview_project_organisation_subsection() -> dict:
    return {
        TITLE: "Project organisation",
        LINK_TITLE: "project_org",
        DATASET: {
            "summary_table": [
                {METRIC: '<a href="../../input" target="_blank">Input data</a>', "value": ""},
                {METRIC: '<a href="../../output/domain_ontology" target="_blank">Domain ontology</a>', "value": ""},
                {METRIC: '<a href="../intermediate/analysis" target="_blank">Analysis data</a>', "value": ""},
                {METRIC: '<a href="images" target="_blank">Analysis plots</a>', "value": ""},
                {METRIC: '<a href="data_profile_reports" target="_blank">Data profiling reports</a>', "value": ""},
                {METRIC: '<a href="data_docs/local_site" target="_blank">Data test report</a>', "value": ""},
                {METRIC: '<a href="../intermediate/data_tests" target="_blank">Data test configurations</a>',
                 "value": ""},
                {METRIC: '<a href="../intermediate/dropped_mappings" target="_blank">Dropped mappings</a>',
                 "value": ""},
                {METRIC: '<a href="logs" target="_blank">Pipeline logs</a>', "value": ""},
            ]
        },
        TEMPLATE: "subsection_content/overview-project-org.html"
    }


def _produce_overview_attributions_subsection() -> dict:
    return {
        TITLE: "Attributions",
        LINK_TITLE: "attributions",
        DATASET: {
        },
        TEMPLATE: "subsection_content/attributions.html"
    }


# SECTION: ALIGNMENT #
def _produce_alignment_detail_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "alignment_steps_detail"
    return {
        TITLE: "Step details",
        LINK_TITLE: "step_details",
        DATASET: {
            "fig_path": _get_figure_path(section_name=section_name,
                                         table_name="step_node_analysis_stacked_bar_chart",
                                         image_format=data_manager.config.image_format),
            "steps_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="steps_detail"
            ),
            TABLE_DESCRIPTION: _load_table_description_data(table_name=table_name),
            UNIQUE_ID: _get_unique_id_for_description_table(section_name=section_name,
                                                            table_name=table_name),
        },
        TEMPLATE: "subsection_content/alignment-details.html",
    }


def _produce_alignment_node_subsection(section_name: str, data_manager: DataManager) -> dict:
    ns_freq_labels = {'namespace': 'ns', 'namespace_count': 'count', 'namespace_freq': 'freq'}
    return {
        TITLE: "Node analysis",
        LINK_TITLE: "node_analysis",
        DATASET: {
            "node_status_fig_path": _get_figure_path(section_name=section_name,
                                                     table_name="node_status",
                                                     image_format=data_manager.config.image_format),
            "node_status_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="node_status",
            ),
            "merged_nodes_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="nodes_merged_ns_freq_analysis",
                rename_columns=ns_freq_labels,
            ),
            "unmapped_nodes_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="nodes_unmapped_ns_freq_analysis",
                rename_columns=ns_freq_labels,
            ),
        },
        TEMPLATE: "subsection_content/alignment-node-analysis.html",
    }


def _produce_merges_subsection_main(section_name: str, data_manager: DataManager) -> dict:
    title = "Merge analysis"
    link_title = "merge_analysis_main"
    return {
        TITLE: title,
        LINK_TITLE: link_title,
        DATASET: {
            TITLE: title,
            LINK_TITLE: link_title,
            "inner_subsections": [
                _produce_merges_subsection_namespaces(section_name=section_name, data_manager=data_manager),
                _produce_merges_subsection_clusters(section_name=section_name, data_manager=data_manager),
            ]
        },
        TEMPLATE: "subsection_content/alignment-merge-analyses.html",
    }


def _produce_merges_subsection_namespaces(section_name: str, data_manager: DataManager) -> dict:
    table_name = "merges_nss_analysis"
    return {
        TITLE: "Namespaces",
        LINK_TITLE: "namespaces",
        DATASET: {
            "analysed_entity": "Merges",
            "analysis_table_template": "data_content/table_merge_analysis.html",
            "analysis_fig_path": _get_figure_path(section_name=section_name,
                                                  table_name="merges_nss_analysis",
                                                  image_format=data_manager.config.image_format),
            "analysis_fig_alt_text": "...",
            TABLE_ANALYSIS: data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_analysis_2": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="merges_nss_canonical_analysis",
            ),
            TABLE_DESCRIPTION: _load_table_description_data(table_name=table_name),
            UNIQUE_ID: _get_unique_id_for_description_table(section_name=section_name,
                                                            table_name=table_name),
        },
        TEMPLATE: "subsection_content/alignment-merge-analysis.html",
    }


def _produce_merges_subsection_clusters(section_name: str, data_manager: DataManager) -> dict:
    return {
        TITLE: "Clusters",
        LINK_TITLE: "cluster",
        DATASET: {
            "analysis_fig_path": _get_figure_path(section_name=section_name,
                                                  table_name="merges_cluster_size_bins",
                                                  image_format=data_manager.config.image_format),
            "analysis_fig_alt_text": "Node merge cluster size bin frequency",
            TABLE_ANALYSIS: data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="merges_cluster_size_description"
            ),
            "table_analysis_2": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="merges_many_nss_merged_to_one_freq",
            ),
        },
        TEMPLATE: "subsection_content/alignment-merge-analysis-clusters.html",
    }


# SECTION: CONNECTIVITY #
def _produce_connectivity_detail_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "connectivity_steps_detail"
    return {
        TITLE: "Step details",
        LINK_TITLE: "step_details",
        DATASET: {
            "fig_path": _get_figure_path(section_name=section_name,
                                         table_name="step_node_analysis_stacked_bar_chart",
                                         image_format=data_manager.config.image_format),
            "steps_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="steps_detail"
            ),
            TABLE_DESCRIPTION: _load_table_description_data(table_name=table_name),
            UNIQUE_ID: _get_unique_id_for_description_table(section_name=section_name,
                                                            table_name=table_name),
        },
        TEMPLATE: "subsection_content/connectivity-details.html",
    }


def _produce_connectivity_node_subsection(section_name: str, data_manager: DataManager) -> dict:
    ns_freq_labels = {'namespace': 'ns', 'namespace_count': 'count', 'namespace_freq': 'freq'}
    return {
        TITLE: "Node analysis",
        LINK_TITLE: "node_analysis",
        DATASET: {
            "node_status_fig_path": _get_figure_path(section_name=section_name,
                                                     table_name="node_status",
                                                     image_format=data_manager.config.image_format),
            "node_status_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="node_status",
            ),
            "connected_nodes_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="nodes_connected_ns_freq_analysis",
                rename_columns=ns_freq_labels,
            ),
            "dangling_nodes_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="nodes_dangling_ns_freq_analysis",
                rename_columns=ns_freq_labels,
            ),
        },
        TEMPLATE: "subsection_content/connectivity-node-analysis.html",
    }


def _produce_connectivity_edge_subsection(section_name: str, data_manager: DataManager) -> dict:
    available_path_overview_table_names = ["ALL"] + [
        path.name.split("_")[-1].replace(".csv", "")
        for path in Path(data_manager.get_analysis_folder_path())
            .rglob('*connectivity_hierarchy_edges_paths_path_lengths_description_*.csv')
        if "ALL.csv" not in path.name
    ]
    available_path_overview_tables = [
        {
            "dataset_name": dataset_name,
            ROWS: data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=f"hierarchy_edges_paths_path_lengths_description_{dataset_name}",
            )
        }
        for dataset_name in available_path_overview_table_names
    ]
    return {
        TITLE: "Hierarchy edge analysis",
        LINK_TITLE: "edge_analysis",
        DATASET: {
            "fig_status": _get_figure_path(section_name=section_name,
                                           table_name="edges_hierarchy_status",
                                           image_format=data_manager.config.image_format),
            "fig_child_parent": _get_figure_path(section_name=section_name,
                                                 table_name="edges_hierarchy_child_parent",
                                                 image_format=data_manager.config.image_format),
            "path_overview_tables": available_path_overview_tables
        },
        TEMPLATE: "subsection_content/connectivity-edge-analysis.html",
    }


# SECTIONS: DATA PROFILING & TESTING #
def _produce_data_file_subsections(section_name: str, data_manager: DataManager) -> List[dict]:
    table_name = TABLE_STATS
    template = "data_content/table_third_party_data.html"
    return [
        {TITLE: "Data: Input", LINK_TITLE: "data_input",
         DATASET: {
             ROWS: data_manager.load_analysis_report_table_as_dict(
                 section_name=f"{section_name}_{DIRECTORY_INPUT}",
                 table_name=table_name,
             ),
             SECTION_NAME: section_name,
         },
         TEMPLATE: template},
        {TITLE: "Data: Intermediate", LINK_TITLE: "data_intermediate",
         DATASET: {
             ROWS: data_manager.load_analysis_report_table_as_dict(
                 section_name=f"{section_name}_{DIRECTORY_INTERMEDIATE}",
                 table_name=table_name,
             ),
             SECTION_NAME: section_name,
         },
         TEMPLATE: template},
        {TITLE: "Data: Output", LINK_TITLE: "data_output",
         DATASET: {
             ROWS: data_manager.load_analysis_report_table_as_dict(
                 section_name=f"{section_name}_{DIRECTORY_OUTPUT}",
                 table_name=table_name,
             ),
             SECTION_NAME: section_name,
         },
         TEMPLATE: template},
    ]


# DESCRIPTION LOADERS #
def _load_table_description_data(table_name: str) -> List[dict]:
    try:
        df = pd.read_csv(os.path.abspath(
            f"../../onto_merger/onto_merger/report/data/table_column_descriptions/{table_name}.csv"))
        return [
            {
                col: row[col]
                for col in list(df)
            }
            for _, row in df.iterrows()
        ]
    except FileNotFoundError as e:
        logger.error(f"Data table missing: {e}")
    return []


# HELPERS #
def _get_section_icon_file_name(section_name: str) -> str:
    return f"icon_{section_name}.png"


def _get_figure_path(section_name: str, table_name: str, image_format: str) -> str:
    return f"images/{section_name}_{table_name}.{image_format}"


def _get_unique_id_for_description_table(section_name: str, table_name: str) -> str:
    """Produce unique IDs that are used in the toggle javascript."""
    return f"{section_name}_{table_name}_description"
