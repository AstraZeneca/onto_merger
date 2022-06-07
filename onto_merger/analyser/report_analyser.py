"""Analyse input and produced data, pipeline processing, data profiling and data tests.

Produce data and figures are presented in the report.
"""

from datetime import datetime
from typing import List

from pandas import DataFrame

from onto_merger.analyser import plotly_utils, report_analyser_utils
from onto_merger.analyser.constants import (
    ANALYSIS_CONNECTED_NSS,
    ANALYSIS_CONNECTED_NSS_CHART,
    ANALYSIS_MAPPED_NSS,
    ANALYSIS_MERGES_NSS,
    ANALYSIS_MERGES_NSS_FOR_CANONICAL,
    ANALYSIS_NODE_NAMESPACE_FREQ,
    ANALYSIS_PROV,
    ANALYSIS_TYPE,
    HEATMAP_MAPPED_NSS,
)
from onto_merger.data.constants import (
    DIRECTORY_ANALYSIS,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_OUTPUT,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_CONNECTIVITY_STEPS_REPORT,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MAPPINGS,
    TABLE_MAPPINGS_DOMAIN,
    TABLE_MERGES_AGGREGATED,
    TABLE_NODES,
    TABLE_NODES_CONNECTED,
    TABLE_NODES_DANGLING,
    TABLE_NODES_DOMAIN,
    TABLE_NODES_MERGED,
    TABLE_NODES_OBSOLETE,
    TABLE_NODES_UNMAPPED,
    TABLE_PIPELINE_STEPS_REPORT,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import (
    AlignmentConfig,
    DataRepository,
    NamedTable,
    RuntimeData,
    convert_runtime_steps_to_named_table,
    format_datetime,
)
from onto_merger.logger.log import get_logger
from onto_merger.report.constants import (
    SECTION_ALIGNMENT,
    SECTION_CONNECTIVITY,
    SECTION_DATA_PROFILING,
    SECTION_DATA_TESTS,
    SECTION_INPUT,
    SECTION_OUTPUT,
    SECTION_OVERVIEW,
)

logger = get_logger(__name__)


class ReportAnalyser:
    """Produce analysis data and illustrations."""

    def __init__(
            self,
            alignment_config: AlignmentConfig,
            data_repo: DataRepository,
            data_manager: DataManager,
            runtime_data: List[RuntimeData],
    ):
        """Initialise the ReportAnalyser class.

        :param alignment_config: The alignment process configuration dataclass.
        :param data_repo: The data repository that stores all input and output tables.
        :param data_manager: The data manager instance.
        """
        self._alignment_config = alignment_config
        self._data_manager = data_manager
        self._data_repo = data_repo
        self._runtime_data = runtime_data
        self._start_date_time = datetime.now()

    # MAIN #
    def produce_report_data(self) -> None:
        """Produce all analysis tables and plots.

        Tables and plots are used in the HTML report.

        :return:
        """
        logger.info("Started producing report analysis...")
        self._produce_input_dataset_analysis()
        self._produce_output_dataset_analysis()
        self._produce_alignment_process_analysis()
        self._produce_connectivity_process_analysis()
        data_test_stats = self._produce_data_testing_analysis()
        data_profiling_stats = self._produce_data_profiling_analysis()
        self._produce_overview_analysis(
            data_profiling_stats=data_profiling_stats,
            data_test_stats=data_test_stats,
        )
        logger.info("Finished producing report analysis.")

    # SECTIONS #
    def _produce_input_dataset_analysis(self) -> None:
        self._produce_in_or_output_dataset_analysis(
            section_dataset_name=SECTION_INPUT,
            node_tables=[
                self._data_repo.get(table_name=TABLE_NODES),
                self._data_repo.get(table_name=TABLE_NODES_OBSOLETE)
            ],
            mappings=self._data_repo.get(table_name=TABLE_MAPPINGS).dataframe,
            edges_hierarchy=self._data_repo.get(table_name=TABLE_EDGES_HIERARCHY).dataframe,
        )

    def _produce_output_dataset_analysis(self) -> None:
        self._produce_in_or_output_dataset_analysis(
            section_dataset_name=SECTION_OUTPUT,
            node_tables=[
                self._data_repo.get(table_name=TABLE_NODES_DOMAIN),
            ],
            mappings=self._data_repo.get(table_name=TABLE_MAPPINGS_DOMAIN).dataframe,
            edges_hierarchy=self._data_repo.get(table_name=TABLE_EDGES_HIERARCHY_POST).dataframe,
        )

    def _produce_in_or_output_dataset_analysis(
            self,
            section_dataset_name: str,
            node_tables: List[NamedTable],
            mappings: DataFrame,
            edges_hierarchy: DataFrame,
    ) -> None:
        logger.info(f"Producing report section '{section_dataset_name}' analysis...")
        tables = []
        if section_dataset_name == SECTION_INPUT:
            tables.append(report_analyser_utils.produce_summary_input(data_repo=self._data_repo))
        if section_dataset_name == SECTION_OUTPUT:
            tables.append(report_analyser_utils.produce_summary_output(data_repo=self._data_repo))
        tables.extend(
            self._produce_node_analyses(
                node_tables=node_tables,
                mappings=mappings,
                edges_hierarchy=edges_hierarchy,
                dataset=section_dataset_name,
            )
        )
        tables.extend(
            self._produce_mapping_analyses(
                mappings=mappings,
                dataset=section_dataset_name,
            )
        )
        tables.extend(
            self._produce_hierarchy_edge_analyses(
                edges=edges_hierarchy,
                dataset=section_dataset_name,
            )
        )
        self._data_manager.save_analysis_named_tables(
            dataset=section_dataset_name,
            tables=tables
        )

    def _produce_alignment_process_analysis(self) -> None:
        section_dataset_name = SECTION_ALIGNMENT
        logger.info(f"Producing report section '{section_dataset_name}' analysis...")
        tables = [
            report_analyser_utils.produce_summary_alignment(data_repo=self._data_repo),
            NamedTable(f"{TABLE_NODES_MERGED}_{ANALYSIS_NODE_NAMESPACE_FREQ}",
                       report_analyser_utils.produce_node_namespace_freq(
                           nodes=self._data_repo.get(table_name=TABLE_NODES_MERGED).dataframe)),
            NamedTable(f"{TABLE_NODES_UNMAPPED}_{ANALYSIS_NODE_NAMESPACE_FREQ}",
                       report_analyser_utils.produce_node_namespace_freq(
                           nodes=self._data_repo.get(table_name=TABLE_NODES_UNMAPPED).dataframe)),
            NamedTable("steps_detail",
                       self._data_repo.get(table_name=TABLE_ALIGNMENT_STEPS_REPORT).dataframe),
        ]
        tables.extend(
            self._produce_merge_analysis(
                merges=self._data_repo.get(table_name=TABLE_MERGES_AGGREGATED).dataframe,
                dataset=section_dataset_name,
            )
        )
        tables.extend(
            report_analyser_utils.produce_runtime_tables(
                table_name=TABLE_ALIGNMENT_STEPS_REPORT,
                section_dataset_name=section_dataset_name,
                data_manager=self._data_manager,
                data_repo=self._data_repo,
            )
        )
        step_report = self._data_repo.get(table_name=TABLE_ALIGNMENT_STEPS_REPORT).dataframe
        report_analyser_utils.produce_step_node_analysis_plot(
            step_report=step_report,
            section_dataset_name=section_dataset_name,
            data_manager=self._data_manager,
            col_count_a="count_mappings",
            col_a="Mapped",
            col_count_b="count_unmapped_nodes",
            col_b="Unmapped",
            b_start_value=step_report["count_unmapped_nodes"].iloc[0],
        )
        self._data_manager.save_analysis_named_tables(
            dataset=section_dataset_name,
            tables=tables
        )

    def _produce_connectivity_process_analysis(self) -> None:
        section_dataset_name = SECTION_CONNECTIVITY
        logger.info(f"Producing report section '{section_dataset_name}' analysis...")
        tables = [
            report_analyser_utils.produce_summary_connectivity(data_repo=self._data_repo),
            NamedTable(f"nodes_connected_{ANALYSIS_NODE_NAMESPACE_FREQ}",
                       report_analyser_utils.produce_node_namespace_freq(
                           nodes=self._data_repo.get(table_name=TABLE_NODES_CONNECTED).dataframe),
                       ),
            NamedTable(f"{TABLE_NODES_DANGLING}_{ANALYSIS_NODE_NAMESPACE_FREQ}",
                       report_analyser_utils.produce_node_namespace_freq(
                           nodes=self._data_repo.get(table_name=TABLE_NODES_DANGLING).dataframe),
                       ),
            NamedTable("steps_detail",
                       self._data_repo.get(table_name=TABLE_CONNECTIVITY_STEPS_REPORT).dataframe),
        ]
        tables.extend(
            report_analyser_utils.produce_runtime_tables(
                table_name=TABLE_CONNECTIVITY_STEPS_REPORT,
                section_dataset_name=section_dataset_name,
                data_manager=self._data_manager,
                data_repo=self._data_repo,
            )
        )
        tables.extend(
            [
                NamedTable(f"hierarchy_edges_paths_{table.name}", table.dataframe)
                for table in
                (
                    report_analyser_utils.produce_hierarchy_edge_path_analysis(
                        hierarchy_edges_paths=self._data_manager.load_table(
                            table_name="connectivity_hierarchy_edges_paths",
                            process_directory=f"{DIRECTORY_OUTPUT}/{DIRECTORY_INTERMEDIATE}/{DIRECTORY_ANALYSIS}"
                        ),
                    )
                )
            ]
        )
        tables.extend(
            [
                NamedTable(f"hierarchy_edges_overview_{table.name}", table.dataframe)
                for table in
                (
                    report_analyser_utils.produce_connectivity_hierarchy_edge_overview_analyses(
                        edges_output=self._data_repo.get(table_name=TABLE_EDGES_HIERARCHY_POST).dataframe,
                        data_manager=self._data_manager,
                    )
                )
            ]
        )
        step_report = self._data_repo.get(table_name=TABLE_CONNECTIVITY_STEPS_REPORT).dataframe
        report_analyser_utils.produce_step_node_analysis_plot(
            step_report=step_report,
            section_dataset_name=section_dataset_name,
            data_manager=self._data_manager,
            col_count_a="count_connected_nodes",
            col_a="Connected",
            col_count_b="count_unmapped_nodes",
            col_b="Dangling",
            b_start_value=step_report["count_unmapped_nodes"].sum(),
        )
        self._data_manager.save_analysis_named_tables(
            dataset=section_dataset_name,
            tables=tables
        )

    def _produce_data_testing_analysis(self) -> DataFrame:
        section_dataset_name = SECTION_DATA_TESTS
        logger.info(f"Producing report section '{section_dataset_name}' analysis...")
        merged_test_stats, dataset_stat_tables = report_analyser_utils.produce_data_testing_table_stats(
            data_manager=self._data_manager
        )
        tables = dataset_stat_tables + [
            report_analyser_utils.produce_summary_data_tests(data_repo=self._data_repo,
                                                             stats=merged_test_stats),
        ]
        self._data_manager.save_analysis_named_tables(
            dataset=section_dataset_name,
            tables=tables
        )
        return merged_test_stats

    def _produce_data_profiling_analysis(self) -> DataFrame:
        section_dataset_name = SECTION_DATA_PROFILING
        logger.info(f"Producing report section '{section_dataset_name}' analysis...")
        merged_profiling_stats, dataset_profiling_tables = report_analyser_utils.produce_data_profiling_table_stats(
            data_manager=self._data_manager
        )
        tables = dataset_profiling_tables + [
            report_analyser_utils.produce_summary_data_profiling(data_repo=self._data_repo,
                                                                 data_profiling_stats=merged_profiling_stats),
        ]
        self._data_manager.save_analysis_named_tables(
            dataset=section_dataset_name,
            tables=tables
        )
        return merged_profiling_stats

    def _produce_overview_analysis(self,
                                   data_profiling_stats: DataFrame,
                                   data_test_stats: DataFrame) -> None:
        section_dataset_name = SECTION_OVERVIEW
        logger.info(f"Producing report section '{section_dataset_name}' analysis...")

        report_analyser_utils.produce_node_status_analyses(
            data_manager=self._data_manager,
            data_repo=self._data_repo
        )

        tables = [
            report_analyser_utils.produce_summary_overview(
                data_manager=self._data_manager,
                data_repo=self._data_repo,
            ),
            report_analyser_utils.produce_validation_overview_analyses(
                data_profiling_stats=data_profiling_stats,
                data_test_stats=data_test_stats,
            )
        ]
        tables.extend(
            [
                NamedTable(f"hierarchy_edge_{table.name}", table.dataframe)
                for table in
                (report_analyser_utils.produce_overview_hierarchy_edge_comparison(
                    data_repo=self._data_repo,
                ))
            ]
        )

        # runtime
        end_date_time = datetime.now()
        analysis_runtime = RuntimeData(
            task="ANALYSIS",
            start=format_datetime(self._start_date_time),
            end=format_datetime(end_date_time),
            elapsed=(end_date_time - self._start_date_time).total_seconds()
        )
        self._runtime_data.append(analysis_runtime)
        run_time_table = convert_runtime_steps_to_named_table(steps=self._runtime_data)
        self._data_repo.update(table=run_time_table)
        self._data_manager.save_table(table=run_time_table)
        tables.extend(
            report_analyser_utils.produce_runtime_tables(
                table_name=TABLE_PIPELINE_STEPS_REPORT,
                section_dataset_name=section_dataset_name,
                data_manager=self._data_manager,
                data_repo=self._data_repo,
            )
        )

        # save produced
        self._data_manager.save_analysis_named_tables(
            dataset=section_dataset_name,
            tables=tables
        )

    # SUBSECTIONS #
    def _produce_node_analyses(self,
                               node_tables: List[NamedTable],
                               mappings: DataFrame,
                               edges_hierarchy: DataFrame,
                               dataset: str) -> List[NamedTable]:
        tables = []
        for table in node_tables:
            # analyse
            analysis_table = report_analyser_utils.produce_node_analyses(
                node_table=table,
                mappings=mappings,
                edges_hierarchy=edges_hierarchy
            )
            tables.append(analysis_table)
            # plot
            plotly_utils.produce_nodes_ns_freq_chart(
                analysis_table=analysis_table.dataframe,
                file_path=self._data_manager.get_analysis_figure_path(
                    dataset=dataset,
                    analysed_table_name=table.name,
                    analysis_table_suffix=analysis_table.name
                )
            )
        return tables

    def _produce_mapping_analyses(self,
                                  mappings: DataFrame,
                                  dataset: str, ) -> List[NamedTable]:
        table_type = TABLE_MAPPINGS

        # plot
        mapping_type_analysis = report_analyser_utils.produce_mapping_analysis_for_type(mappings=mappings)
        plotly_utils.produce_mapping_type_freq_chart(
            analysis_table=mapping_type_analysis,
            file_path=self._data_manager.get_analysis_figure_path(
                dataset=dataset,
                analysed_table_name=table_type,
                analysis_table_suffix="type_analysis"
            )
        )

        # plot
        mapped_nss_heatmap_data = report_analyser_utils.produce_edges_analysis_for_mapped_or_connected_nss_heatmap(
            edges=mappings,
            prune=False,
            directed_edge=False
        )
        plotly_utils.produce_edge_heatmap(
            analysis_table=mapped_nss_heatmap_data,
            file_path=self._data_manager.get_analysis_figure_path(
                dataset=dataset,
                analysed_table_name=table_type,
                analysis_table_suffix=ANALYSIS_MAPPED_NSS
            )
        )

        return [
            NamedTable(f"{table_type}_{ANALYSIS_PROV}",
                       report_analyser_utils.produce_mapping_analysis_for_prov(mappings=mappings)),
            NamedTable(f"{table_type}_{ANALYSIS_TYPE}", mapping_type_analysis),
            NamedTable(f"{table_type}_{ANALYSIS_MAPPED_NSS}",
                       report_analyser_utils.produce_mapping_analysis_for_mapped_nss(mappings=mappings)),
            NamedTable(f"{table_type}_{HEATMAP_MAPPED_NSS}",
                       report_analyser_utils.produce_mapping_analysis_for_mapped_nss(mappings=mappings)),
            # index=True
        ]

    def _produce_hierarchy_edge_analyses(self,
                                         edges: DataFrame,
                                         dataset: str) -> List[NamedTable]:
        table_type = TABLE_EDGES_HIERARCHY
        connected_nss = report_analyser_utils.produce_source_to_target_analysis_for_directed_edge(edges=edges)
        tables = [
            NamedTable(f"{table_type}_{ANALYSIS_CONNECTED_NSS}",
                       report_analyser_utils.produce_hierarchy_edge_analysis_for_connected_nss(edges=edges)),
            NamedTable(f"{table_type}_{ANALYSIS_CONNECTED_NSS_CHART}", connected_nss),

        ]
        plotly_utils.produce_hierarchy_nss_stacked_bar_chart(
            analysis_table=connected_nss,
            file_path=self._data_manager.get_analysis_figure_path(
                dataset=dataset,
                analysed_table_name=table_type,
                analysis_table_suffix=ANALYSIS_CONNECTED_NSS_CHART
            )
        )
        return tables

    def _produce_merge_analysis(self,
                                merges: DataFrame,
                                dataset: str) -> List[NamedTable]:
        table_type = "merges"
        merged_nss = report_analyser_utils.produce_source_to_target_analysis_for_directed_edge(edges=merges)
        # plot
        plotly_utils.produce_merged_nss_stacked_bar_chart(
            analysis_table=merged_nss,
            file_path=self._data_manager.get_analysis_figure_path(
                dataset=dataset,
                analysed_table_name=table_type,
                analysis_table_suffix=ANALYSIS_MERGES_NSS
            )
        )
        tables = [
            NamedTable(f"{table_type}_{ANALYSIS_MERGES_NSS}", merged_nss),
            NamedTable(f"{table_type}_{ANALYSIS_MERGES_NSS_FOR_CANONICAL}",
                       report_analyser_utils.produce_merge_analysis_for_merged_nss_for_canonical(merges=merges)),
            NamedTable(f"{table_type}_{ANALYSIS_MERGES_NSS}", merged_nss),
        ]
        tables.extend(
            report_analyser_utils.produce_merge_cluster_analysis(merges_aggregated=merges,
                                                                 data_manager=self._data_manager)
        )
        return tables
