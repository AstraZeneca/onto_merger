from onto_merger.report.data.section_summary_descriptions import *

# DUMMY DATA #
data_table_node_summary = [
    {"ns": "MONDO", "input": "22,123 (19.80%)", "merged": "", "only_connected": "", "dangling": ""},
    {"ns": "MEDDRA", "input": "67,999 (58.65%)", "merged": "10.00%", "only_connected": "54.30%", "dangling": "34.34%"},
    {"ns": "ORPHANET", "input": "9,123 (8.19%)", "merged": "23.00%", "only_connected": "11.34%", "dangling": "12.34%"},
]
data_table_node_analysis = [
    {"ns": "MONDO", "count": 22000, "freq": "20.00%", "coverage_mapping": "20.00%",
     "coverage_edge_hierarchy": "100.00%"},
    {"ns": "MEDDRA", "count": 22000, "freq": "45.32%", "coverage_mapping": "10.00%",
     "coverage_edge_hierarchy": "100.00%"},
    {"ns": "ORPHANET", "count": 22000, "freq": "12.00%", "coverage_mapping": "5.00%",
     "coverage_edge_hierarchy": "100.00%"},
]
data_table_node_obs_ns_freq = [
    {"ns": "MONDO", "count": 22000, "freq": "1.00%", "coverage_mapping": "95.55%", "coverage_edge_hierarchy": "0%"},
]
data_table_mapping_analysis = [
    {"node_nss": "{ MONDO }", "count": "22,123", "freq": "20.00%", "types": "{ eqv, xref }", "provs": "{ MONDO }"},
    {"node_nss": "{ MEDDRA, MONDO }", "count": "22,123", "freq": "20.00%", "types": "{ eqv, xref }",
     "provs": "{ MONDO }"},
    {"node_nss": "{ DOID, MONDO }", "count": "22,123", "freq": "20.00%", "types": "{ eqv, xref }",
     "provs": "{ MONDO }"},
]
data_table_edge_analysis = [
    {"sub_to_sup": "MONDO to MONDO", "count": "22,123", "freq": "20.00%", "provs": "MONDO"},
    {"sub_to_sup": "MEDDRA to MEDDRA", "count": "77,123", "freq": "65.00%", "provs": "MONDO"},
]
data_table_merge_analysis = [
    {"canonical_ns": "MONDO", "merged_ns": "MEDDRA", "count": "22,123", "freq": "45.00%"},
    {"canonical_ns": "MONDO", "merged_ns": "DOID", "count": "2,123", "freq": "10.00%"},
    {"canonical_ns": "MONDO", "merged_ns": "MESH", "count": "4,123", "freq": "23.00%"},
]
data_table_node_mapping_coverage = [
    {"ns": "MONDO to MONDO",
     "total_count": "22,123", "total_freq": "20.00%",
     "covered_count": "2,123", "covered_freq": "20.00%",
     "uncovered_count": "9,,123", "uncovered_freq": "20.00%",
     },
    {"ns": "MONDO to MONDO",
     "total_count": "22,123", "total_freq": "20.00%",
     "covered_count": "2,123", "covered_freq": "20.00%",
     "uncovered_count": "9,,123", "uncovered_freq": "20.00%",
     },
    {"ns": "MONDO to MONDO",
     "total_count": "22,123", "total_freq": "20.00%",
     "covered_count": "2,123", "covered_freq": "20.00%",
     "uncovered_count": "9,,123", "uncovered_freq": "20.00%",
     },
]
table_steps_alignment = [
    {"step": 0, "mapping_type_group": "", "source": "",
     "count_unmapped_nodes": "115,934", "count_merged_nodes": "0", "count_mappings": "",
     "count_nodes_one_source_to_many_target": "", },
    {"step": 1, "mapping_type_group": "equivalence", "source": "MONDO",
     "count_unmapped_nodes": "94,022", "count_merged_nodes": "20,871", "count_mappings": "20,871",
     "count_nodes_one_source_to_many_target": "0", },
    {"step": 2, "mapping_type_group": "equivalence", "source": "MEDDRA",
     "count_unmapped_nodes": "73,151", "count_merged_nodes": "5,000", "count_mappings": "6,000",
     "count_nodes_one_source_to_many_target": "1,000", },
]
table_steps_connectivity = [
    {"step": 1, "source": "MEDDRA",
     "count_unmapped_nodes": "66,594", "count_reachable_unmapped_nodes": "20,871", "count_connected_nodes": "10,000",
     "count_available_edges": "55,000", "count_produced_edges": "12,000", },
    {"step": 2, "source": "ORPHANET",
     "count_unmapped_nodes": "6,594", "count_reachable_unmapped_nodes": "2,871", "count_connected_nodes": "1,000",
     "count_available_edges": "5,000", "count_produced_edges": "2,000", },
]
tables_data_profiling = {
    "input": [
        {
            "type": "node",
            "name": "nodes.csv",
            "link": "http://"
        },
        {
            "type": "node",
            "name": "node_obsolete.csv",
            "link": "http://"
        },
        {
            "type": "mapping",
            "name": "mappings.csv",
            "link": "http://"
        },
        {
            "type": "edge",
            "name": "edges_hierarchy.csv",
            "link": "http://"
        },
    ],
    "intermediate": [
        {
            "type": "node",
            "name": "nodes_dangling.csv",
            "link": "http://"
        },
        {
            "type": "node",
            "name": "node_merged.csv",
            "link": "http://"
        },
        {
            "type": "node",
            "name": "node_only_connected.csv",
            "link": "http://"
        },
        {
            "type": "node",
            "name": "node_unmapped.csv",
            "link": "http://"
        },
        {
            "type": "mapping",
            "name": "mappings_updated.csv",
            "link": "http://"
        },
        {
            "type": "mapping",
            "name": "mappings_obsolete_to_current.csv",
            "link": "http://"
        },
        {
            "type": "mapping",
            "name": "merges_with_meta_data.csv",
            "link": "http://"
        },
        {
            "type": "mapping",
            "name": "merges_aggregated.csv",
            "link": "http://"
        },
        {
            "type": "edge",
            "name": "edges_hierarchy_post.csv",
            "link": "http://"
        },
    ],
    "output": [
        {
            "type": "node",
            "name": "nodes.csv",
            "link": "http://"
        },
        {
            "type": "mapping",
            "name": "merges.csv",
            "link": "http://"
        },
        {
            "type": "mapping",
            "name": "mappings.csv",
            "link": "http://"
        },
        {
            "type": "edge",
            "name": "edges_hierarchy.csv",
            "link": "http://"
        },
    ],
}
runtime_table = [
    {"metric": "Loading", "values": "2 sec"},
    {"metric": "Loading", "values": "2 sec"},
    {"metric": "Loading", "values": "2 sec"},
    {"metric": "Loading", "values": "2 sec"}
]
config_json_str = \
"""{
    "domain_node_type": "Disease",
    "seed_ontology_name": "MONDO",
    "mappings": {
     "type_groups": {
         "equivalence": [
             "equivalent_to",
             "merge"
         ],
         "database_reference": [
             "database_cross_reference",
             "xref"
         ],
         "label_match": []
     }
}"""


# SECTIONS #
data_overview = {
    "title": "Overview",
    "link_title": "overview",
    "logo": "icon_overview.png",
    "subsections": [
        {
            "title": "At glance",
            "link_title": "glance",
            "dataset": {
                "description": {
                    "title": "About",
                    "text": "...",
                },
                "summary_table": [
                    {"metric": "Number of ", "values": "123"},
                    {"metric": "Number of ", "values": "123"},
                    {"metric": "Number of ", "values": "123"},
                ],
            },
            "template": "subsection_content/section-summary.html"
        },
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "table_analysis": data_table_node_summary,
                "table_description": table_description_node_summary,
                "unique_id": "data_table_node_summary_description",
            },
            "template": "subsection_content/overview-summary.html"
        },
        {"title": "Pipeline", "link_title": "pipeline",
         "dataset": {
             "start_date_time": "???",
             "end_date_time": "???",
             "total_runtime": "1 hour 23 minutes",
             "gant_img": "plotly_gant_processing.svg",
             "runtime_table": runtime_table,
             "runtime_summary_table": [
                 {"metric": "START", "values": "2022.01.01 10:00:05"},
                 {"metric": "END", "values": "2022.01.01 11:00:05"},
                 {"metric": "RUNTIME", "values": "1:00:05"},
             ]
         },
         "template": "subsection_content/subsection-runtime.html"},
        {"title": "Configuration", "link_title": "configuration",
         "dataset": {
             "about": "The config JSON ...",
             "config_json": config_json_str
         },
         "template": "subsection_content/overview-configuration.html"},
        {"title": "Validation", "link_title": "validation", "dataset": {},
         "template": "subsection_content/overview-validation.html"},
    ],
}
data_input = {
    "title": "Input",
    "link_title": "input",
    "logo": "icon_input.png",
    "subsections": [
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "description": {
                    "title": "About",
                    "text": "...",
                },
                "summary_table": [
                    {"metric": "Number of ", "values": "123"},
                    {"metric": "Number of ", "values": "123"},
                    {"metric": "Number of ", "values": "123"},
                ],
            },
            "template": "subsection_content/section-summary.html"
        },
        {"title": "Nodes", "link_title": "nodes",
         "dataset": {
             "table_analysis": data_table_node_analysis,
             "table_description": table_description_node_analysis,
             "unique_id": "input_data_table_node_analysis_description",
             "is_obsolete": False,
         },
         "template": "subsection_content/dataset-nodes.html"},
        {"title": "Nodes (Obsolete)", "link_title": "nodes_obsolete",
         "dataset": {
             "table_analysis": data_table_node_obs_ns_freq,
             "table_description": table_description_node_analysis,
             "unique_id": "input_data_table_node_obs_analysis_description",
             "is_obsolete": True,
         },
         "template": "subsection_content/dataset-nodes.html"},
        {"title": "Mappings", "link_title": "mappings",
         "dataset": {
             "analysed_entity": "Mappings",
             "analysis_table_template": "data_content/table_mapping_analysis.html",
             "analysis_fig_path": "plotly_heat_map.svg",
             "analysis_fig_alt_text": "foo",
             "table_analysis": data_table_mapping_analysis,
             "table_description": table_description_mapping_analysis,
             "unique_id": "input_data_table_mapping_analysis_description",
         },
         "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"},
        {"title": "Hierarchy edges", "link_title": "edges_hierarchy",
         "dataset": {
             "analysed_entity": "Hierarchy edges",
             "analysis_table_template": "data_content/table_edge_analysis.html",
             "analysis_fig_path": "plotly_heat_map.svg",
             "analysis_fig_alt_text": "foo",
             "table_analysis": data_table_edge_analysis,
             "table_description": table_description_edge_hierarchy_analysis,
             "unique_id": "input_data_table_edge_hierarchy_analysis_description",
         },
         "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"},
    ],
}
data_output = {
    "title": "Output",
    "link_title": "output",
    "logo": "icon_output.png",
    "subsections": [
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "description": {
                    "title": "About",
                    "text": "...",
                },
                "summary_table": [
                    {"metric": "Number of ", "values": "123"},
                    {"metric": "Number of ", "values": "123"},
                    {"metric": "Number of ", "values": "123"},
                ],
            },
            "template": "subsection_content/section-summary.html"
        },
        {"title": "Nodes", "link_title": "nodes",
         "dataset": {
             "table_analysis": data_table_node_analysis,
             "table_description": table_description_node_analysis,
             "unique_id": "output_data_table_node_analysis_description",
             "is_obsolete": False,
         },
         "template": "subsection_content/dataset-nodes.html"},
        {"title": "Merges", "link_title": "merges",
         "dataset": {
             "analysed_entity": "Merges",
             "analysis_table_template": "data_content/table_merge_analysis.html",
             "analysis_fig_path": "plotly_heat_map.svg",
             "analysis_fig_alt_text": "foo",
             "table_analysis": data_table_merge_analysis,
             "table_description": table_description_merge_analysis,
             "unique_id": "output_data_table_merge_analysis_description",
         },
         "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"},
        {"title": "Mappings", "link_title": "mappings",
         "dataset": {
             "analysed_entity": "Mappings",
             "analysis_table_template": "data_content/table_mapping_analysis.html",
             "analysis_fig_path": "plotly_heat_map.svg",
             "analysis_fig_alt_text": "foo",
             "table_analysis": data_table_mapping_analysis,
             "table_description": table_description_mapping_analysis,
             "unique_id": "input_data_table_mapping_analysis_description",
         },
         "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"},
        {"title": "Hierarchy edges", "link_title": "edges_hierarchy",
         "dataset": {
             "analysed_entity": "Hierarchy edges",
             "analysis_table_template": "data_content/table_edge_analysis.html",
             "analysis_fig_path": "plotly_heat_map.svg",
             "analysis_fig_alt_text": "foo",
             "table_analysis": data_table_edge_analysis,
             "table_description": table_description_edge_hierarchy_analysis,
             "unique_id": "input_data_table_edge_hierarchy_analysis_description",
         },
         "template": "subsection_content/dataset-entity-analysis-with-chart-and-description.html"},
    ],
}
data_alignment = {
    "title": "Alignment",
    "link_title": "alignment",
    "logo": "icon_deduplication.png",
    "subsections": [
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "description": {
                    "title": "About",
                    "text": '''
            <p>The <b>alignment process</b> ... bla ... more on this in RDT.</p>
            <p>The <b>alignment process</b> ... bla ... more on this in RDT.</p>
            ''',
                },
                "summary_table": [
                    {"metric": "Number of steps", "values": "24"},
                    {"metric": "Number of sources", "values": "12"},
                    {"metric": "Mapping type groups used", "values": "2"},
                    {"metric": "Input nodes", "values": "99,000"},
                    {"metric": "Unmapped nodes", "values": "50,000"},
                    {"metric": "Unmapped nodes (%)", "values": "51.00%"},
                    {"metric": "Merged nodes", "values": "49,000"},
                    {"metric": "Merged nodes (%)", "values": "49.00%"},
                    {"metric": "Merges", "values": "49,000"},
                ],
            },
            "template": "subsection_content/section-summary.html"
        },
        {"title": "Details", "link_title": "details",
         "dataset": {
             "steps_table": table_steps_alignment,
             "table_description": table_description_alignment_steps,
             "unique_id": "data_table_alignment_steps_description",
         },
         "template": "subsection_content/alignment-details.html"},
        {"title": "Processing", "link_title": "pipeline",
         "dataset": {
             "start_date_time": "???",
             "end_date_time": "???",
             "total_runtime": "1 hour 23 minutes",
             "gant_img": "plotly_gant_processing.svg",
             "runtime_table": runtime_table,
             "runtime_summary_table": [
                 {"metric": "START", "values": "2022.01.01 10:00:05"},
                 {"metric": "END", "values": "2022.01.01 11:00:05"},
                 {"metric": "RUNTIME", "values": "1:00:05"},
             ]
         },
         "template": "subsection_content/subsection-runtime.html"},
    ],
}
data_connectivity = {
    "title": "Connectivity",
    "link_title": "connectivity",
    "logo": "icon_connectivity.png",
    "subsections": [
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "description": {
                    "title": "About",
                    "text": '''
        <p>The <b>connectivity process</b> ... bla ... more on this in RDT.</p>
        <p>The <b>process</b> ... bla ... more on this in RDT.</p>
        ''',
                },
                "summary_table": [
                    {"metric": "Number of steps", "values": "24"},
                    {"metric": "Number of sources", "values": "12"},
                    {"metric": "Mapping type groups used", "values": "2"},
                    {"metric": "Input nodes", "values": "99,000"},
                    {"metric": "Unmapped nodes", "values": "50,000"},
                    {"metric": "Unmapped nodes (%)", "values": "51.00%"},
                    {"metric": "Merged nodes", "values": "49,000"},
                    {"metric": "Merged nodes (%)", "values": "49.00%"},
                    {"metric": "Merges", "values": "49,000"},
                ],
            },
            "template": "subsection_content/section-summary.html"
        },
        {"title": "Details", "link_title": "details",
         "dataset": {
             "steps_table": table_steps_alignment,
             "table_description": table_description_connectivity_steps,
             "unique_id": "data_table_connectivity_steps_description",
         },
         "template": "subsection_content/connectivity-details.html"},
        {"title": "Processing", "link_title": "pipeline",
         "dataset": {
             "start_date_time": "???",
             "end_date_time": "???",
             "total_runtime": "1 hour 23 minutes",
             "gant_img": "plotly_gant_processing.svg",
             "runtime_table": runtime_table,
             "runtime_summary_table": [
                 {"metric": "START", "values": "2022.01.01 10:00:05"},
                 {"metric": "END", "values": "2022.01.01 11:00:05"},
                 {"metric": "RUNTIME", "values": "1:00:05"},
             ]
         },
         "template": "subsection_content/subsection-runtime.html"},
    ],
}
data_profiling = {
    "title": "Data Profiling",
    "link_title": "data_profiling",
    "logo": "icon_data_profiling.png",
    "subsections": [
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "description": {
                    "title": "About",
                    "text": "The data testing process ... Pandas Profiling is a bla... ... more on this in RDT.",
                },
                "summary_table": [
                    {"metric": "Time taken", "values": "1 min 23 seconds"},
                    {"metric": "Profiled tables", "values": "18"},
                    {"metric": "Pandas profiling version", "values": "1.2.3"},
                ],
            },
            "template": "subsection_content/section-summary.html"
        },
        {"title": "Data: Input", "link_title": "data_input",
         "dataset": tables_data_profiling["input"],
         "template": "data_content/table_third_party_data.html"},
        {"title": "Data: Intermediate", "link_title": "data_intermediate",
         "dataset": tables_data_profiling["intermediate"],
         "template": "data_content/table_third_party_data.html"},
        {"title": "Data: Output", "link_title": "data_output",
         "dataset": tables_data_profiling["output"],
         "template": "data_content/table_third_party_data.html"},
    ],
}
data_tests = {
    "title": "Data Tests",
    "link_title": "data_tests",
    "logo": "icon_data_tests.png",
    "subsections": [
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "description": {
                    "title": "About",
                    "text": "The data testing process ... GE is a bla... ... more on this in RDT.",
                },
                "summary_table": [
                    {"metric": "Time taken", "values": "2 min 23 seconds"},
                    {"metric": "Number of tables tested", "values": "18"},
                    {"metric": "Number of tests run", "values": "123"},
                    {"metric": "Number of failed tests (input data)", "values": "0"},
                    {"metric": "Number of failed tests (intermediate data)", "values": "0"},
                    {"metric": "Number of failed tests (output data)", "values": "0"},
                    {"metric": "GE version", "values": "1.2.3"},
                ],
            },
            "template": "subsection_content/section-summary.html"
        },
        {"title": "Data: Input", "link_title": "data_input",
         "dataset": tables_data_profiling["input"],
         "template": "data_content/table_third_party_data.html"},
        {"title": "Data: Intermediate", "link_title": "data_intermediate",
         "dataset": tables_data_profiling["intermediate"],
         "template": "data_content/table_third_party_data.html"},
        {"title": "Data: Output", "link_title": "data_output",
         "dataset": tables_data_profiling["output"],
         "template": "data_content/table_third_party_data.html"},
    ],
}
