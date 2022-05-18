"""OntoMerger HTML report."""

from datetime import datetime

from jinja2 import Environment, FileSystemLoader

from onto_merger.data.data_manager import DataManager
from onto_merger.report.data.section_data_loader import load_report_data
from onto_merger.logger.log import get_logger
from onto_merger.report.dummy_data import *  # todo remove

logger = get_logger(__name__)


def produce_report(data_manager: DataManager) -> str:
    """Produce the merged ontology and alignment process analysis report HTML.

    :return: The path of the HTML report.
    """
    # load data
    report_data = _load_data_dummy()
    # report_data = load_report_data(data_manager=data_manager)

    # load template and render with analysis data
    rendered_report = _produce_report_content(report_data=report_data)

    # save report
    report_path = data_manager.save_merged_ontology_report(content=rendered_report)

    return report_path


def _load_data_dummy() -> dict:
    data = {
        "date": f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
        "version": "foo",
        "title": "Report",
        "overview_data": data_overview,
        "input_data": data_input,
        "output_data": data_output,
        "alignment_data": data_alignment,
        "connectivity_data": data_connectivity,
        "data_profiling": data_profiling,
        "data_tests": data_tests,
    }
    return data


def _produce_report_content(report_data: dict) -> str:
    template_loader = FileSystemLoader(searchpath="./")
    template_environment = Environment(loader=template_loader)
    report_template = "templates/report.html"
    template = template_environment.get_template(report_template)
    report_content = template.render(report_data)
    return report_content
