
data_table_node_ns_freq = [
    {"ns": "MONDO", "count": "22,123", "freq": "20.00%"},
    {"ns": "MONDO", "count": "22,123", "freq": "20.00%"},
    {"ns": "MONDO", "count": "22,123", "freq": "20.00%"},
]
data_table_mapping_ns_freq = [
    {"ns": "MONDO to MONDO", "count": "22,123", "freq": "20.00%"},
    {"ns": "MONDO to MONDO", "count": "22,123", "freq": "20.00%"},
    {"ns": "MONDO to MONDO", "count": "22,123", "freq": "20.00%"},
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



data_profiling_input = [
    {
        "type": "node",
        "name": "nodes_merged",
        "link": "http://localhost:63342/onto_merger/tests/test_data/output/report/data_profile_reports/nodes_merged_report.html"
    },
    {
        "type": "node_obsolete",
        "name": "node_obsolete",
        "link": "http://localhost:63342/onto_merger/tests/test_data/output/report/data_profile_reports/nodes_merged_report.html"
    },
    {
        "type": "mapping",
        "name": "mapping",
        "link": "http://localhost:63342/onto_merger/tests/test_data/output/report/data_profile_reports/nodes_merged_report.html"
    },
    {
        "type": "edge",
        "name": "edge",
        "link": "http://localhost:63342/onto_merger/tests/test_data/output/report/data_profile_reports/nodes_merged_report.html"
    },
]

# # # SECTIONS # # #

data_overview = {
    "title": "Overview",
    "link_title": "overview",
    "logo": "icon_overview.png",
    "summary": {

    },
    "processing": {
        "gant_img": "plotly_gant_processing.svg"
    },
    "validation": {

    },
}

data_input = {
    "title": "Input",
    "link_title": "input_data",
    "logo": "icon_input.png",
    "nodes_ns": {
        "title": "Namespace",
        "rows": data_table_node_ns_freq
    },
    "nodes_coverage_mappings": {
        "title": "Namespace",
        "rows": data_table_node_mapping_coverage
    },
    "nodes_coverage_edges": {
        "title": "Namespace",
        "rows": data_table_node_mapping_coverage
    },
    "mappings_ns": {
        "title": "Source to Target Namespace",
        "rows": data_table_mapping_ns_freq
    }
}

data_output = {
    "title": "Output",
    "link_title": "output_data",
    "logo": "icon_output.png",
    "nodes_ns": {
        "title": "Namespace",
        "rows": data_table_node_ns_freq
    },
    "nodes_coverage_mappings": {
        "title": "Namespace",
        "rows": data_table_node_mapping_coverage
    },
    "nodes_coverage_edges": {
        "title": "Namespace",
        "rows": data_table_node_mapping_coverage
    },
    "mappings_ns": {
        "title": "Source to Target Namespace",
        "rows": data_table_mapping_ns_freq
    }
}

data_alignment = {
    "title": "Alignment",
    "link_title": "alignment",
    "logo": "icon_deduplication.png",
}

data_connectivity = {
    "title": "Connectivity",
    "link_title": "connectivity",
    "logo": "icon_connectivity.png",
}

data_profiling = {
    "title": "Data Profiling",
    "link_title": "data_profiling",
    "logo": "icon_data_profiling.png",
    "attribution": "Pandas Profiling is a bla...",
    "tables": {
        "input": {
            "title": "foo",
            "rows": data_profiling_input,
        },
        "intermediate": {
            "title": "foo",
            "rows": data_profiling_input[2:3],
        },
        "output": {
            "title": "foo",
            "rows": data_profiling_input[1:-1],
        },
    },
}

data_tests = {
    "title": "Data Tests",
    "link_title": "data_tests",
    "logo": "icon_data_tests.png",
    "attribution": "GE is a bla...",
    "tables": {
        "input": {
            "title": "foo",
            "rows": [],
        },
        "intermediate": {
            "title": "foo",
            "rows": [],
        },
        "output": {
            "title": "foo",
            "rows": [],
        },
    },
}