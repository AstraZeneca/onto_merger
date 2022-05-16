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

# STATIC #
data_table_node_summary_description = [
    {"column": "Node Origin",
     "description": "The ontology (or namespace) the node originates from."},
    {"column": "Input",
     "description": "Nodes that assume to belong to the same domain that most likely "
                    + "contain duplicated, connected and overlapping nodes."},
    {"column": "Merged",
     "description": "Nodes that are mapped and merged onto other nodes."},
    {"column": "Only connected",
     "description": "Nodes that are not merged onto other nodes, but are connected to the hierarchy."},
    {"column": "Dangling",
     "description": "Nodes that are not neither merged nor connected."},
]
data_table_node_analysis_description = [
    {"column": "Origin",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
    {"column": "Mapping coverage",
     "description": "..."},
    {"column": "Hierarchy coverage",
     "description": "..."},
]
data_table_mapping_analysis_description = [
    {"column": "Mapped nodes",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
    {"column": "Types",
     "description": "..."},
    {"column": "Provenances",
     "description": "..."},
]
data_table_merge_analysis_description = [
    {"column": "Canonical",
     "description": "..."},
    {"column": "Merged",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
]
data_table_edge_hierarchy_analysis_description = [
    {"column": "Sub to Super",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
    {"column": "Provenance",
     "description": "..."},
]
data_table_alignment_steps_description = [
    {"column": "Step",
     "description": ""},
    {"column": "Mapping type",
     "description": ""},
    {"column": "Source",
     "description": ""},
    {"column": "Unmapped",
     "description": ""},
    {"column": "Merged",
     "description": ""},
    {"column": "Mappings",
     "description": ""},
    {"column": "Dropped",
     "description": ""},
]
data_table_connectivity_steps_description = [
    {"column": "Step",
     "description": ""},
    {"column": "Source",
     "description": ""},
    {"column": "Unmapped",
     "description": ""},
    {"column": "Reachable",
     "description": ""},
    {"column": "Connected",
     "description": ""},
    {"column": "Edges (available)",
     "description": ""},
    {"column": "Edges (produced)",
     "description": ""},
]

# SUBSECTION #

# SECTIONS #
data_overview = {
    "title": "Overview",
    "link_title": "overview",
    "logo": "icon_overview.png",
    "subsections": [
        {
            "title": "Summary",
            "link_title": "summary",
            "dataset": {
                "node_summary": data_table_node_summary,
                "node_summary_description": data_table_node_summary_description,
            },
            "template": "subsection_content/overview-summary.html"
        },
        {"title": "Pipeline", "link_title": "pipeline",
         "dataset": {
             "start_date_time": "???",
             "end_date_time": "???",
             "total_runtime": "1 hour 23 minutes",
             "gant_img": "plotly_gant_processing.svg"
         },
         "template": "subsection_content/overview-pipeline.html"},
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
             "table_description": data_table_node_analysis_description,
             "is_obsolete": False,
         },
         "template": "subsection_content/dataset-nodes.html"},
        {"title": "Nodes (Obsolete)", "link_title": "nodes_obsolete",
         "dataset": {
             "table_analysis": data_table_node_obs_ns_freq,
             "table_description": data_table_node_analysis_description,
             "is_obsolete": True,
         },
         "template": "subsection_content/dataset-nodes.html"},
        {"title": "Mappings", "link_title": "mappings",
         "dataset": {
             "table_analysis": data_table_mapping_analysis,
             "table_description": data_table_mapping_analysis_description,
         },
         "template": "subsection_content/dataset-mappings.html"},
        {"title": "Hierarchy edges", "link_title": "edges_hierarchy",
         "dataset": {
             "table_analysis": data_table_edge_analysis,
             "table_description": data_table_edge_hierarchy_analysis_description,
         },
         "template": "subsection_content/dataset-edges-hierarchy.html"},
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
             "table_description": data_table_node_analysis_description,
             "is_obsolete": False,
         },
         "template": "subsection_content/dataset-nodes.html"},
        {"title": "Merges", "link_title": "merges",
         "dataset": {
             "table_analysis": data_table_merge_analysis,
             "table_description": data_table_merge_analysis_description,
         },
         "template": "subsection_content/dataset-merges.html"},
        {"title": "Mappings", "link_title": "mappings",
         "dataset": {
             "table_analysis": data_table_mapping_analysis,
             "table_description": data_table_mapping_analysis_description,
         },
         "template": "subsection_content/dataset-mappings.html"},
        {"title": "Hierarchy edges", "link_title": "edges_hierarchy",
         "dataset": {
             "table_analysis": data_table_edge_analysis,
             "table_description": data_table_edge_hierarchy_analysis_description,
         },
         "template": "subsection_content/dataset-edges-hierarchy.html"},
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
             "description_table": data_table_alignment_steps_description,
         },
         "template": "subsection_content/alignment-details.html"},
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
             "description_table": data_table_connectivity_steps_description,
         },
         "template": "subsection_content/connectivity-details.html"},
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
