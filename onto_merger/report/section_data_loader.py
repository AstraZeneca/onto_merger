"""Loads data for report sections."""
import json
import os
from datetime import datetime
from typing import List

import pandas as pd

from onto_merger.analyser.constants import TABLE_SECTION_SUMMARY, TABLE_STATS
from onto_merger.data.constants import DIRECTORY_INTERMEDIATE, DIRECTORY_INPUT, DIRECTORY_OUTPUT
from onto_merger.data.data_manager import DataManager
from onto_merger.report.constants import SECTION_INPUT, SECTION_OUTPUT, SECTION_DATA_TESTS, \
    SECTION_DATA_PROFILING, SECTION_CONNECTIVITY, SECTION_OVERVIEW, SECTION_ALIGNMENT
from onto_merger.version import __version__
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


# MAIN #
def load_report_data(data_manager: DataManager) -> dict:
    return {
        "date": f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
        "version": __version__,
        "title": "Report",
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
        "title": title,
        "link_title": section_name,
        "logo": _get_section_icon_file_name(section_name=section_name),
        "subsections": [
                           _produce_section_summary_subsection(section_name=section_name, data_manager=data_manager),
                       ] + subsections
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
            _produce_overview_config_subsection(section_name=section_name, data_manager=data_manager),
            _produce_overview_validation_subsection(section_name=section_name, data_manager=data_manager),
            _produce_overview_project_organisation_subsection(section_name=section_name, data_manager=data_manager),
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
        "title": "Summary",
        "link_title": section_name,
        "dataset": {
            # "description": _load_section_summary_description_data(section_name=section_name),
            "summary_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=TABLE_SECTION_SUMMARY
            ),
        },
        "template": "subsection_content/section-summary.html"
    }


def _produce_runtime_info_subsection(section_name: str, data_manager: DataManager) -> dict:
    section_data = {
        "title": "Processing",
        "link_title": "pipeline",
        "dataset": {
            "gantt_img": f"images/{section_name}_pipeline_steps_report_gantt_chart.svg",
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
            "unique_id": _get_unique_id_for_description_table(
                section_name=section_name,
                table_name=f"{section_name}_pipeline_steps"
            ),
        },
        "template": "subsection_content/subsection-runtime.html"
    }
    return section_data


# SUBSECTIONS #
def _produce_nodes_subsection(section_name: str, data_manager: DataManager, is_obsolete: bool) -> dict:
    table_name = "nodes_general_analysis" if is_obsolete is False else "nodes_obsolete_general_analysis"
    return {
        "title": "Nodes" if is_obsolete is False else "Nodes (Obsolete)",
        "link_title": "nodes" if is_obsolete is False else "nodes_obsolete",
        "dataset": {
            "node_status_fig_path": _get_figure_path(section_name=section_name, table_name="node_status"),
            "node_status_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="node_status"
            ),
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_description": _load_table_description_data(table_name="nodes_general_analysis"),
            "unique_id": _get_unique_id_for_description_table(section_name=section_name,
                                                              table_name=table_name),
            "is_obsolete": is_obsolete,
        },
        "template": "subsection_content/dataset-nodes.html"
    }


def _produce_mappings_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "mappings_mapped_nss_analysis"
    return {
        "title": "Mappings",
        "link_title": "mappings",
        "dataset": {
            "analysed_entity": "Mappings",
            "analysis_table_template": "data_content/table_mapping_analysis.html",
            "heatmap_fig_path": _get_figure_path(section_name=section_name, table_name=table_name),
            "heatmap_fig_alt_text": "...",
            "types_fig_path": _get_figure_path(section_name=section_name, table_name="mappings_type_analysis"),
            "types_fig_alt_text": "...",
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_description": _load_table_description_data(table_name="mappings_mapped_nss_analysis"),
            "unique_id": _get_unique_id_for_description_table(section_name=section_name,
                                                              table_name=table_name),
        },
        "template": "subsection_content/mapping-analysis.html"
    }


def _produce_edges_hierarchy_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "edges_hierarchy_connected_nss_analysis"
    return {
        "title": "Hierarchy edges",
        "link_title": "edges_hierarchy",
        "dataset": {
            "analysed_entity": "Hierarchy edges",
            "analysis_table_template": "data_content/table_edge_analysis.html",
            "analysis_fig_path": _get_figure_path(section_name=section_name,
                                                  table_name=f"{table_name}_chart"),
            "analysis_fig_alt_text": "...",
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_description": _load_table_description_data(table_name=table_name),
            "unique_id": _get_unique_id_for_description_table(section_name=section_name,
                                                              table_name=table_name),
        },
        "template": "subsection_content/edges-analysis.html",
    }


# SECTION: OVERVIEW #
def _produce_overview_config_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Configuration", "link_title": "configuration",
        "dataset": {
            "dataset_id": data_manager.get_project_folder_path().split("/")[-1],
            "config_json": json.dumps(data_manager.load_alignment_config().as_dict, indent=4),
        },
        "template": "subsection_content/overview-configuration.html"
    }


def _produce_overview_validation_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Profiling & Validation",
        "link_title": "profiling_and_validation",
        "dataset": {
            "rows": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="data_profiling_and_tests_summary",
            ),
        },
        "template": "data_content/table_validation_summary.html"
    }


def _produce_overview_nodes_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Node analysis",
        "link_title": "nodes",
        "dataset": {
            "node_status_fig_path": _get_figure_path(section_name=section_name, table_name="node_status"),
            "node_status_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="node_status"
            ),
        },
        "template": "subsection_content/overview-node-summary.html"
    }


def _produce_overview_edges_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Hierarchy analysis",
        "link_title": "hierarchy_analysis",
        "dataset": {
            "table_general_comparison": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="hierarchy_edge_general_comparison"
            ),
            "table_children_count_comparison": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="hierarchy_edge_children_count_comparison"
            ),
        },
        "template": "subsection_content/overview-edge-summary.html"
    }


def _produce_overview_project_organisation_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Project organisation",
        "link_title": "project_org",
        "dataset": {
            "summary_table": [
                {"metric": '<a href="../../input" target="_blank">Input data</a>', "value": ""},
                {"metric": '<a href="../../output/domain_ontology" target="_blank">Domain ontology</a>', "value": ""},
                {"metric": '<a href="../intermediate/analysis" target="_blank">Analysis data</a>', "value": ""},
                {"metric": '<a href="images" target="_blank">Analysis plots</a>', "value": ""},
                {"metric": '<a href="data_profile_reports" target="_blank">Data profiling reports</a>', "value": ""},
                {"metric": '<a href="data_docs" target="_blank">Data test report</a>', "value": ""},
                {"metric": '<a href="../intermediate/data_tests" target="_blank">Data test configurations</a>',
                 "value": ""},
                {"metric": '<a href="../intermediate/dropped_mappings" target="_blank">Dropped mappings</a>',
                 "value": ""},
                {"metric": '<a href="logs" target="_blank">Pipeline logs</a>', "value": ""},
            ]
        },
        "template": "subsection_content/overview-project-org.html"
    }


def _produce_overview_attributions_subsection() -> dict:
    return {
        "title": "Attributions",
        "link_title": "attributions",
        "dataset": {
        },
        "template": "subsection_content/attributions.html"
    }


# SECTION: ALIGNMENT #
def _produce_alignment_detail_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "alignment_steps_detail"
    return {
        "title": "Step details",
        "link_title": "step_details",
        "dataset": {
            "fig_path": _get_figure_path(section_name=section_name,
                                         table_name="step_node_analysis_stacked_bar_chart"),
            "steps_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="steps_detail"
            ),
            "table_description": _load_table_description_data(table_name=table_name),
            "unique_id": _get_unique_id_for_description_table(section_name=section_name,
                                                              table_name=table_name),
        },
        "template": "subsection_content/alignment-details.html",
    }


def _produce_alignment_node_subsection(section_name: str, data_manager: DataManager) -> dict:
    ns_freq_labels = {'namespace': 'ns', 'namespace_count': 'count', 'namespace_freq': 'freq'}
    return {
        "title": "Node analysis",
        "link_title": "node_analysis",
        "dataset": {
            "node_status_fig_path": _get_figure_path(section_name=section_name,
                                                     table_name="node_status"),
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
        "template": "subsection_content/alignment-node-analysis.html",
    }


def _produce_merges_subsection_main(section_name: str, data_manager: DataManager) -> dict:
    title = "Merge analysis"
    link_title = "merge_analysis_main"
    return {
        "title": title,
        "link_title": link_title,
        "dataset": {
            "title": title,
            "link_title": link_title,
                "inner_subsections": [
                    _produce_merges_subsection_namespaces(section_name=section_name, data_manager=data_manager),
                    _produce_merges_subsection_clusters(section_name=section_name, data_manager=data_manager),
                ]
        },
        "template": "subsection_content/alignment-merge-analyses.html",
    }


def _produce_merges_subsection_namespaces(section_name: str, data_manager: DataManager) -> dict:
    table_name = "merges_nss_analysis"
    return {
        "title": "Namespaces",
        "link_title": "namespaces",
        "dataset": {
            "analysed_entity": "Merges",
            "analysis_table_template": "data_content/table_merge_analysis.html",
            "analysis_fig_path": _get_figure_path(section_name=section_name, table_name="merges_nss_analysis"),
            "analysis_fig_alt_text": "...",
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_analysis_2": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="merges_nss_canonical_analysis",
            ),
            "table_description": _load_table_description_data(table_name=table_name),
            "unique_id": _get_unique_id_for_description_table(section_name=section_name,
                                                              table_name=table_name),
        },
        "template": "subsection_content/alignment-merge-analysis.html",
    }


def _produce_merges_subsection_clusters(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Clusters",
        "link_title": "cluster",
        "dataset": {
            "analysis_fig_path": _get_figure_path(section_name=section_name, table_name="merges_cluster_size_bins"),
            "analysis_fig_alt_text": "Node merge cluster size bin frequency",
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="merges_cluster_size_description"
            ),
            "table_analysis_2": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="merges_many_nss_merged_to_one_freq",
            ),
        },
        "template": "subsection_content/alignment-merge-analysis-clusters.html",
    }


# SECTION: CONNECTIVITY #
def _produce_connectivity_detail_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = "connectivity_steps_detail"
    return {
        "title": "Step details",
        "link_title": "step_details",
        "dataset": {
            "fig_path": _get_figure_path(section_name=section_name,
                                         table_name="step_node_analysis_stacked_bar_chart"),
            "steps_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name="steps_detail"
            ),
            "table_description": _load_table_description_data(table_name=table_name),
            "unique_id": _get_unique_id_for_description_table(section_name=section_name,
                                                              table_name=table_name),
        },
        "template": "subsection_content/connectivity-details.html",
    }


def _produce_connectivity_node_subsection(section_name: str, data_manager: DataManager) -> dict:
    ns_freq_labels = {'namespace': 'ns', 'namespace_count': 'count', 'namespace_freq': 'freq'}
    return {
        "title": "Node analysis",
        "link_title": "node_analysis",
        "dataset": {
            "node_status_fig_path": _get_figure_path(section_name=section_name,
                                                     table_name="node_status"),
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
        "template": "subsection_content/connectivity-node-analysis.html",
    }


# SECTIONS: DATA PROFILING & TESTING #
def _produce_data_file_subsections(section_name: str, data_manager: DataManager) -> List[dict]:
    table_name = TABLE_STATS
    template = "data_content/table_third_party_data.html"
    return [
        {"title": "Data: Input", "link_title": "data_input",
         "dataset": {
             "rows": data_manager.load_analysis_report_table_as_dict(
                 section_name=f"{section_name}_{DIRECTORY_INPUT}",
                 table_name=table_name,
             ),
             "section_name": section_name,
         },
         "template": template},
        {"title": "Data: Intermediate", "link_title": "data_intermediate",
         "dataset": {
             "rows": data_manager.load_analysis_report_table_as_dict(
                 section_name=f"{section_name}_{DIRECTORY_INTERMEDIATE}",
                 table_name=table_name,
             ),
             "section_name": section_name,
         },
         "template": template},
        {"title": "Data: Output", "link_title": "data_output",
         "dataset": {
             "rows": data_manager.load_analysis_report_table_as_dict(
                 section_name=f"{section_name}_{DIRECTORY_OUTPUT}",
                 table_name=table_name,
             ),
             "section_name": section_name,
         },
         "template": template},
    ]


# DESCRIPTION LOADERS #
def _load_table_description_data(table_name: str) -> List[dict]:
    """Table descriptions explaining columns of tables presented in the report."""
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


def _get_figure_path(section_name: str, table_name: str) -> str:
    return f"images/{section_name}_{table_name}.svg"


def _get_unique_id_for_description_table(section_name: str, table_name: str) -> str:
    """Unique IDs are used in the toggle javascript."""
    return f"{section_name}_{table_name}_description"
