"""Loads data for report sections."""
from typing import List

from data.constants import DIRECTORY_INTERMEDIATE, DIRECTORY_INPUT, DIRECTORY_OUTPUT
from onto_merger.data.data_manager import DataManager
from onto_merger.report.report_generator import load_section_summary_description_data, load_table_description_data
from onto_merger.report.data.constants import SECTION_INPUT, SECTION_OUTPUT, SECTION_DATA_TESTS, \
    SECTION_DATA_PROFILING, SECTION_CONNECTIVITY, SECTION_OVERVIEW, SECTION_ALIGNMENT
from onto_merger.analyser.constants import TABLE_SECTION_SUMMARY, TABLE_NODE_ANALYSIS, TABLE_NODE_OBSOLETE_ANALYSIS, \
    TABLE_MAPPING_ANALYSIS, TABLE_EDGE_HIERARCHY_ANALYSIS, TABLE_STATS, TABLE_MERGE_ANALYSIS

# todo
from onto_merger.report.dummy_data import *


# SECTIONS #
def produce_section(title: str, section_name: str, subsections: List[dict]) -> dict:
    return {
        "title": title,
        "link_title": section_name,
        "logo": get_section_icon_file_name(section_name=section_name),
        "subsections": subsections
    }


def load_input_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_INPUT
    return produce_section(
        title="Input",
        section_name=section_name,
        subsections=[
            produce_section_summary_subsection(section_name=section_name, data_manager=data_manager),
            produce_nodes_subsection(section_name=section_name, data_manager=data_manager, is_obsolete=False),
            produce_nodes_subsection(section_name=section_name, data_manager=data_manager, is_obsolete=True),
            produce_mappings_subsection(section_name=section_name, data_manager=data_manager),
            produce_edges_hierarchy_subsection(section_name=section_name, data_manager=data_manager),
        ]
    )


def load_output_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_OUTPUT
    return produce_section(
        title="Output",
        section_name=section_name,
        subsections=[
            produce_section_summary_subsection(section_name=section_name, data_manager=data_manager),
            produce_nodes_subsection(section_name=section_name, data_manager=data_manager, is_obsolete=False),
            produce_merges_subsection(section_name=section_name, data_manager=data_manager),
            produce_mappings_subsection(section_name=section_name, data_manager=data_manager),
            produce_edges_hierarchy_subsection(section_name=section_name, data_manager=data_manager),
        ]
    )


def load_overview_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_OVERVIEW
    return produce_section(
        title="Overview",
        section_name=section_name,
        subsections=[
            produce_section_summary_subsection(section_name=section_name, data_manager=data_manager),
            produce_overview_summary_subsection(section_name=section_name, data_manager=data_manager),
            produce_pipeline_info_subsection(section_name=section_name, data_manager=data_manager),
            produce_overview_config_subsection(section_name=section_name, data_manager=data_manager),
            produce_overview_validation_subsection(section_name=section_name, data_manager=data_manager),
        ]
    )


def load_alignment_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_ALIGNMENT
    return produce_section(
        title="Alignment",
        section_name=section_name,
        subsections=[
            produce_section_summary_subsection(section_name=section_name, data_manager=data_manager),
            produce_process_detail_subsection(section_name=section_name, data_manager=data_manager),
            produce_pipeline_info_subsection(section_name=section_name, data_manager=data_manager),
        ]
    )


def load_connectivity_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_CONNECTIVITY
    return produce_section(
        title="Connectivity",
        section_name=section_name,
        subsections=[
            produce_section_summary_subsection(section_name=section_name, data_manager=data_manager),
            produce_process_detail_subsection(section_name=section_name, data_manager=data_manager),
            produce_pipeline_info_subsection(section_name=section_name, data_manager=data_manager),
        ]
    )


def load_data_profiling_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_DATA_PROFILING
    subsections = [produce_section_summary_subsection(section_name=section_name, data_manager=data_manager)]
    subsections.extend(
        produce_data_file_subsections(
            section_name=section_name,
            data_manager=data_manager
        )
    )
    return produce_section(
        title="Data Profiling",
        section_name=section_name,
        subsections=subsections
    )


def load_data_testing_section_data(data_manager: DataManager) -> dict:
    section_name = SECTION_DATA_TESTS
    subsections = [produce_section_summary_subsection(section_name=section_name, data_manager=data_manager)]
    subsections.extend(
        produce_data_file_subsections(
            section_name=section_name,
            data_manager=data_manager
        )
    )
    return produce_section(
        title="Data Tests",
        section_name=section_name,
        subsections=subsections
    )


# SUBSECTIONS #
def produce_section_summary_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Summary",
        "link_title": section_name,
        "dataset": {
            "description": load_section_summary_description_data(section_name=section_name),
            "summary_table": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=TABLE_SECTION_SUMMARY
            ),
        },
        "template": "subsection_content/section-summary.html"
    }


def produce_nodes_subsection(section_name: str, data_manager: DataManager, is_obsolete: bool) -> dict:
    table_name = TABLE_NODE_ANALYSIS if is_obsolete is False else TABLE_NODE_OBSOLETE_ANALYSIS
    return {
        "title": "Nodes" if is_obsolete is False else "Nodes (Obsolete)",
        "link_title": "nodes" if is_obsolete is False else "nodes_obsolete",
        "dataset": {
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_description": load_table_description_data(table_name=table_name),
            "unique_id": get_unique_id_for_description_table(section_name=section_name,
                                                             table_name=table_name),
            "is_obsolete": is_obsolete,
        },
        "template": "subsection_content/dataset-nodes.html"
    }


def produce_mappings_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = TABLE_MAPPING_ANALYSIS
    return {
        "title": "Mappings",
        "link_title": "mappings",
        "dataset": {
            "analysed_entity": "Mappings",
            "analysis_table_template": "data_content/table_mapping_analysis.html",
            "analysis_fig_path": "plotly_heat_map.svg",
            "analysis_fig_alt_text": "...",
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_description": load_table_description_data(table_name=table_name),
            "unique_id": get_unique_id_for_description_table(section_name=section_name,
                                                             table_name=table_name),
        },
        "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"
    }


def produce_merges_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = TABLE_MERGE_ANALYSIS
    return {
        "title": "Merges",
        "link_title": "merges",
        "dataset": {
            "analysed_entity": "Merges",
            "analysis_table_template": "data_content/table_merge_analysis.html",
            "analysis_fig_path": "plotly_heat_map.svg",
            "analysis_fig_alt_text": "...",
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_description": load_table_description_data(table_name=table_name),
            "unique_id": get_unique_id_for_description_table(section_name=section_name,
                                                             table_name=table_name),
        },
        "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"
    }


def produce_edges_hierarchy_subsection(section_name: str, data_manager: DataManager) -> dict:
    table_name = TABLE_EDGE_HIERARCHY_ANALYSIS
    return {
        "title": "Hierarchy edges", "link_title": "edges_hierarchy",
        "dataset": {
            "analysed_entity": "Hierarchy edges",
            "analysis_table_template": "data_content/table_edge_analysis.html",
            "analysis_fig_path": "plotly_heat_map.svg",
            "analysis_fig_alt_text": "...",
            "table_analysis": data_manager.load_analysis_report_table_as_dict(
                section_name=section_name,
                table_name=table_name
            ),
            "table_description": load_table_description_data(table_name=table_name),
            "unique_id": get_unique_id_for_description_table(section_name=section_name,
                                                             table_name=table_name),
        },
        "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"}


# todo
def produce_overview_config_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Configuration", "link_title": "configuration",
        "dataset": {
            "about": "The config JSON ...",
            "config_json": "..."
        },
        "template": "subsection_content/overview-configuration.html"
    }


# todo
def produce_overview_validation_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Validation",
        "link_title": "validation",
        "dataset": {},
        "template": "subsection_content/overview-validation.html"
    }


def produce_overview_summary_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Summary",
        "link_title": "summary",
        "dataset": {
            "table_analysis": data_table_node_summary,
            "table_description": table_description_node_summary,
            "unique_id": "data_table_node_summary_description",
        },
        "template": "subsection_content/overview-summary.html"
    }


# todo
def produce_pipeline_info_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Processing",
        "link_title": "pipeline",
        "dataset": {
            "start_date_time": "???",
            "end_date_time": "???",
            "total_runtime": "1 hour 23 minutes",
            "gantt_img": "plotly_gant_processing.svg",
            "runtime_table": [],
            "runtime_summary_table": [
                {"metric": "START", "values": "2022.01.01 10:00:05"},
                {"metric": "END", "values": "2022.01.01 11:00:05"},
                {"metric": "RUNTIME", "values": "1:00:05"},
            ]
        },
        "template": "subsection_content/subsection-runtime.html"
    }


# todo
def produce_process_detail_subsection(section_name: str, data_manager: DataManager) -> dict:
    return {
        "title": "Details",
        "link_title": "details",
        "dataset": {
            "steps_table": [],
            "table_description": [],
            "unique_id": "data_table_connectivity_steps_description",
        }
    }


def produce_data_file_subsections(section_name: str, data_manager: DataManager) -> List[dict]:
    table_name = TABLE_STATS
    template = "data_content/table_third_party_data.html"
    return [
        {"title": "Data: Input", "link_title": "data_input",
         "dataset": data_manager.load_analysis_report_table_data_stats(
             section_name=DIRECTORY_INPUT,
             replace_col_name=section_name,
             table_name=table_name
         ),
         "template": template},
        {"title": "Data: Intermediate", "link_title": "data_intermediate",
         "dataset": data_manager.load_analysis_report_table_data_stats(
             section_name=DIRECTORY_INTERMEDIATE,
             replace_col_name=section_name,
             table_name=table_name
         ),
         "template": template},
        {"title": "Data: Output", "link_title": "data_output",
         "dataset": data_manager.load_analysis_report_table_data_stats(
             section_name=DIRECTORY_OUTPUT,
             replace_col_name=section_name,
             table_name=table_name
         ),
         "template": template},
    ]


# HELPERS #
def get_section_icon_file_name(section_name: str) -> str:
    return f"icon_{section_name}.png"


def get_unique_id_for_description_table(section_name: str, table_name: str) -> str:
    """Unique IDs are used in the toggle javascript."""
    return f"{section_name}_{table_name}_description"
