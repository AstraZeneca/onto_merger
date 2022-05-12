"""Runs the alignment and connection process, input and output validation and produces reports."""
from typing import List

from onto_merger.alignment import hierarchy_utils, mapping_utils, merge_utils
from onto_merger.alignment.alignment_manager import AlignmentManager
from onto_merger.alignment_config.validator import validate_alignment_configuration
from onto_merger.analyser import analysis_util, pandas_profiler
from onto_merger.analyser.merged_ontology_analyser import MergedOntologyAnalyser
from onto_merger.data.constants import (
    TABLE_EDGES_HIERARCHY,
    TABLE_MERGES,
    TABLE_MERGES_AGGREGATED,
    TABLE_NODES,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import DataRepository
from onto_merger.data_testing.ge_runner import GERunner
from onto_merger.logger.log import setup_logger


class Pipeline:
    """Data repository containing all input and processed DataFrames."""

    """The data repository that stores the input and output tables with their
     corresponding names (types)."""
    _data_repo: DataRepository = DataRepository()

    def __init__(self, project_folder_path: str) -> None:
        """Initialise the Pipeline class.

        :param project_folder_path: The directory path where the project inputs are
        stored.
        """
        self._project_folder_path = DataManager.get_absolute_path(project_folder_path)
        self._short_project_name = self._project_folder_path.split("/")[-1]
        self._data_manager = DataManager(project_folder_path=self._project_folder_path)
        self._alignment_config = self._data_manager.load_alignment_config()
        self.logger = setup_logger(module_name=__name__, file_name=self._data_manager.get_log_file_path())
        self._source_alignment_order: List[str] = []

    def run_alignment_and_connection_process(self) -> None:
        """Run the alignment and connectivity process, validate inputs and outputs, produce analysis.

        :return:
        """
        self.logger.info("Started running alignment and connection process for " + f"'{self._short_project_name}'")

        # (1) VALIDATE CONFIG
        self._validate_alignment_config()

        # (2) LOAD AND CHECK INPUT DATA
        self._process_input_data()

        # (3) RUN ALIGNMENT >> merges
        self._align_nodes()

        # (4) AGGREGATE MERGES >> merges updated
        self._aggregate_merges()

        # (5) RUN CONNECTIVITY >> hierarchy edges
        self._connect_nodes()

        # (6) FINALISE OUTPUTS >> unmapped, dangling, only connected
        # | add NS to all outputs
        self._finalise_outputs()

        # (7) VALIDATE: output data
        self._validate_outputs()

        # (8) PRODUCE REPORT
        self._produce_report()

        self.logger.info("Finished running alignment and connection process for " + f"'{self._short_project_name}'")

    def _validate_alignment_config(self) -> None:
        """Run the alignment configuration JSON schema validator.

        Raises an exception if the config is invalid.

        :return:
        """
        self.logger.info("Started validating alignment config...")
        config_json_is_valid = validate_alignment_configuration(alignment_config=self._alignment_config.as_dict)
        if config_json_is_valid is False:
            raise Exception
        self.logger.info("Finished validating alignment config.")

    def _process_input_data(self) -> None:
        """Load, preprocess, profile and validate the input data.

        Raises an exception if the inputs are invalid (missing or fail data tests).
        Results (loaded tables) are stored in the data repository.

        :return:
        """
        self.logger.info("Started processing input data...")

        # load  and preprocess input tables: add namespaces for downstream processing
        self._data_repo.update(
            tables=analysis_util.add_namespace_column_to_loaded_tables(tables=self._data_manager.load_input_tables())
        )

        # profile input tables todo uncomment
        pandas_profiler.profile_tables(tables=self._data_repo.get_input_tables(), data_manager=self._data_manager)

        # validate input tables todo uncomment
        # GERunner(
        #     alignment_config=self._alignment_config,
        #     ge_base_directory=self._data_manager.get_data_tests_path(),
        # ).run_ge_tests(named_tables=self._data_repo.get_input_tables())

        self.logger.info("Finished processing input data.")

    def _align_nodes(self) -> None:
        """Run the alignment process.

        Results (merge table and alignment steps) are stored in the data repository.

        :return:
        """
        self.logger.info("Started aligning nodes...")

        alignment_results, source_alignment_order = AlignmentManager(
            alignment_config=self._alignment_config,
            data_repo=self._data_repo,
            data_manager=self._data_manager,
        ).align_nodes()
        self._data_repo.update(tables=alignment_results.get_output_tables())
        self._source_alignment_order.extend(source_alignment_order)
        self.logger.info("Finished aligning nodes...")

    def _aggregate_merges(self) -> None:
        """Run the merge aggregation process (merges targets become only canonical IDs).

        Results (aggregated merges) are stored in the data repository.

        :return:
        """
        self.logger.info("Started aggregating merges...")
        table_aggregated_merges = merge_utils.produce_named_table_aggregate_merges(
            merges=self._data_repo.get(TABLE_MERGES).dataframe,
            alignment_priority_order=self._source_alignment_order,
        )
        table_merged_nodes = merge_utils.produce_named_table_merged_nodes(merges=table_aggregated_merges.dataframe)
        table_unmapped_nodes = mapping_utils.produce_named_table_unmapped_nodes(
            nodes=self._data_repo.get(TABLE_NODES).dataframe,
            merges=self._data_repo.get(TABLE_MERGES).dataframe,
        )
        self._data_repo.update(tables=[table_aggregated_merges, table_merged_nodes, table_unmapped_nodes])
        self.logger.info("Finished aggregating merges.")

    def _connect_nodes(self) -> None:
        """Run the connectivity process to produce the domain ontology hierarchy.

        Results (hierarchy edges) are stored in the data repository.

        :return:
        """
        self.logger.info("Started connecting nodes...")
        self._data_repo.update(
            tables=hierarchy_utils.connect_nodes(
                alignment_config=self._alignment_config,
                source_alignment_order=self._source_alignment_order,
                data_repo=self._data_repo,
            )
        )
        self.logger.info("Finished connecting nodes.")

    def _finalise_outputs(self) -> None:
        """Produce the final merged ontology and pre-processes tables for validation.

        Results (merged, unmapped, connected, danging nodes) are stored in the data
        repository.

        :return:
        """
        self.logger.info("Started finalising outputs...")

        # compute: unmapped, dangling, only connected
        self._data_repo.update(
            tables=[
                mapping_utils.produce_named_table_unmapped_nodes(
                    nodes=self._data_repo.get(TABLE_NODES).dataframe,
                    merges=self._data_repo.get(TABLE_MERGES_AGGREGATED).dataframe,
                ),
                hierarchy_utils.produce_table_nodes_only_connected(
                    hierarchy_edges=self._data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
                    merges=self._data_repo.get(TABLE_MERGES_AGGREGATED).dataframe,
                ),
                hierarchy_utils.produce_table_nodes_dangling(
                    nodes=self._data_repo.get(TABLE_NODES).dataframe,
                    hierarchy_edges=self._data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
                    merges=self._data_repo.get(TABLE_MERGES_AGGREGATED).dataframe,
                ),
            ]
        )

        #  add NS to all outputs
        self._data_repo.update(
            tables=analysis_util.add_namespace_column_to_loaded_tables(tables=self._data_repo.get_output_tables())
        )

        # save all outputs
        self._data_manager.save_tables(tables=self._data_repo.get_output_tables())

        # save final tables to domain ontology folder
        self._data_manager.save_domain_ontology_tables(data_repo=self._data_repo)

        self.logger.info("Finished finalising outputs.")

    def _validate_outputs(self) -> None:
        """Run the output data validation process.

        :return:
        """
        self.logger.info("Started validating produced data...")

        # run data tests
        GERunner(
            alignment_config=self._alignment_config,
            ge_base_directory=self._data_manager.get_data_tests_path(),
        ).run_ge_tests(named_tables=self._data_repo.get_output_tables())

        # move data docs to report folder
        self._data_manager.move_data_docs_to_reports()

        self.logger.info("Finished validating produced data.")

    def _produce_report(self) -> None:
        """Run the alignment and connectivity evaluation process.

        :return:
        """
        # profile outputs
        pandas_profiler.profile_tables(tables=self._data_repo.get_output_tables(), data_manager=self._data_manager)

        # produce HTML report
        report_path = MergedOntologyAnalyser(
            data_repo=self._data_repo, data_manager=self._data_manager
        ).produce_report()
        self.logger.info(f"Saved report to {report_path}.")
