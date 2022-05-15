"""OntoMerger HTML report."""

from jinja2 import Environment, FileSystemLoader


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

# data
data = {
    "date": "2022.05.13",
    "version": "latest",
    "title": "Report",
    "overview_data": {
        "foo": "bar"
    },
    "input_data": {
        "title": "Input",
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
    },
    "output_data": {
        "title": "Output",
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
}

# load and render template
templateLoader = FileSystemLoader(searchpath="./")
templateEnv = Environment(loader=templateLoader)
TEMPLATE_FILE = "templates/report.html"
template = templateEnv.get_template(TEMPLATE_FILE)
report_content = template.render(data)

# save report
file_path = "onto_merger_report.html"
with open(file_path, "w") as f:
    f.write(report_content)


