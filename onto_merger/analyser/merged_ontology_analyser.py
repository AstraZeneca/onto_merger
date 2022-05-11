from jinja2 import Template
from pandas import DataFrame

from onto_merger.analyser import analysis_util
from onto_merger.data.constants import (
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_NODES,
    TABLE_NODES_CONNECTED_ONLY,
    TABLE_NODES_DANGLING,
    TABLE_NODES_MERGED,
    TABLE_CONNECTIVITY_STEPS_REPORT)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import DataRepository, NamedTable
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


class MergedOntologyAnalyser:
    def __init__(self, data_repo: DataRepository, data_manager: DataManager):
        """Initialises the MergedOntologyAnalyser class.

        :param data_repo: The data repository containing input and output data
        tables.
        :param data_manager: The data manager.
        """
        self._data_manager = data_manager
        self._data_repo = data_repo

    def produce_report(self) -> str:
        """Produces the merged ontology and alignment process analysis report
        HTML.

        :return: The path of the HTML report.
        """
        # render report with evaluation data
        report_content = Template(_report_template).render(self._evaluate_merged_ontology())
        # save report
        return self._data_manager.save_merged_ontology_report(content=report_content)

    def _evaluate_merged_ontology(self) -> dict:
        """Produces a dictionary HTML table used to produce the HTML report.

        :return: The report data as a dictionary.
        """
        data_dic = {
            # "summary_table": self._produce_summary_table().to_html(index=False)
            "node_categories": [
                self._produce_detail_report_data_for_node_category(
                    node_category="Input",
                    description="The set of input nodes that assume to belong "
                    + "to the same domain that most likely contain"
                    + "duplicated, connected and overlapping nodes.",
                    node_table=self._data_repo.get(TABLE_NODES),
                ),
                self._produce_detail_report_data_for_node_category(
                    node_category="Merged",
                    description="Nodes that are mapped and merged onto other nodes.",
                    node_table=self._data_repo.get(TABLE_NODES_MERGED),
                ),
                self._produce_detail_report_data_for_node_category(
                    node_category="Only connected",
                    description="Nodes that are not merged onto other nodes, but are " + "connected to the hierarchy.",
                    node_table=self._data_repo.get(TABLE_NODES_CONNECTED_ONLY),
                ),
                self._produce_detail_report_data_for_node_category(
                    node_category="Dangling",
                    description="Nodes that are not merged or connected.",
                    node_table=self._data_repo.get(TABLE_NODES_DANGLING),
                ),
            ],
            "alignment_steps": self._data_repo.get(TABLE_ALIGNMENT_STEPS_REPORT).dataframe.to_html(index=False),
            "connectivity_steps": self._data_repo.get(TABLE_CONNECTIVITY_STEPS_REPORT).dataframe.to_html(index=False),
        }
        return data_dic

    def _produce_detail_report_data_for_node_category(
        self, node_category: str, description: str, node_table: NamedTable
    ):
        """Produces a detailed report for each node category, to be displayed in the
        report HTML.

        :param node_category: The category name.
        :param description: Brief description of the node category.
        :param node_table: The node table.
        :return: A tuple with the report data points.
        """
        input_nodes = self._data_repo.get(TABLE_NODES).dataframe
        table_count = len(node_table.dataframe)
        diff_ratio = (table_count / len(input_nodes)) * 100
        count_and_ratio = f"{table_count:,d} ({diff_ratio:.2f}%)"
        namespace_distribution_table: DataFrame = analysis_util.produce_table_node_namespace_distribution(
            node_table=node_table.dataframe
        )
        pandas_profiling_link = (
            None
            if namespace_distribution_table is None
            else self._data_manager.get_profiled_table_report_path(table_name=node_table.name, relative_path=True)
        )
        return (
            node_category,
            description,
            count_and_ratio,
            None if namespace_distribution_table is None else namespace_distribution_table.to_html(index=False),
            pandas_profiling_link,
            table_count,
            diff_ratio,
        )


"""The HTML report template string."""
_report_template: str = """
<!DOCTYPE html>
<html>
<head>
<title>Onto-merger report</title>
</head>
<body>

<h1>Onto-merger report</h1>


<h2>Alignment steps</h2>

{{ alignment_steps }}

<h2>Connectivity steps</h2>

{{ connectivity_steps }}

<h2>Nodes</h2>

{% for node_category in node_categories %}

<hr>

<h3>Nodes: {{ node_category[0] }}</h3>

<ul>
  <li><b>Description:</b> {{  node_category[1] }}</li>
  <li><b>Size:</b> {{  node_category[2] }}</li>
  {% if node_category[3] is not none %}
  <li><a href="{{  node_category[4] }}">Profiling report</a></li>
  <li><b>Namespace distribution:<br><br>{{  node_category[3] }}<br></b></li>
  {% else %}
  {% endif %}
</ul>

{% endfor %}

<hr>

</body>
</html>
"""
