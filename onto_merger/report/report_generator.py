"""OntoMerger HTML report."""

from datetime import datetime

from jinja2 import Environment, FileSystemLoader

# todo remove
from onto_merger.report.dummy_data import *

# data
data = {
    "date": f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
    "version": "latest",
    "title": "Report",
    "overview_data": data_overview,
    "input_data": data_input,
    "output_data": data_output,
    "alignment_data": data_alignment,
    "connectivity_data": data_connectivity,
    "data_profiling": data_profiling,
    "data_tests": data_tests,
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


