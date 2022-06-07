"""OntoMerger HTML report."""

from jinja2 import Environment, FileSystemLoader

from onto_merger.data.data_manager import DataManager
from onto_merger.logger.log import get_logger
from onto_merger.report.section_data_loader import load_report_data

logger = get_logger(__name__)


def produce_report(data_manager: DataManager) -> str:
    """Produce the merged ontology and alignment process analysis report HTML.

    :return: The path of the HTML report.
    """
    # load data
    report_data = load_report_data(data_manager=data_manager)

    # load template and render with analysis data
    template_search_path = data_manager.get_file_system_loader_path()
    rendered_report = _produce_report_content(report_data=report_data,
                                              template_search_path=template_search_path)

    # save report
    report_path = data_manager.save_merged_ontology_report(content=rendered_report,
                                                           template_search_path=template_search_path)

    return report_path


def _produce_report_content(report_data: dict, template_search_path: str) -> str:
    template_loader = FileSystemLoader(searchpath=template_search_path)
    template_environment = Environment(loader=template_loader)
    report_template = "templates/report.html"
    template = template_environment.get_template(report_template)
    report_content = template.render(report_data)
    return report_content
