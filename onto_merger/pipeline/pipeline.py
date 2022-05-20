"""Runs the alignment and connection process, input and output validation and produces reports."""
from datetime import datetime
from typing import List

from onto_merger.alignment import hierarchy_utils, merge_utils
from onto_merger.alignment.alignment_manager import AlignmentManager
from onto_merger.alignment_config.validator import validate_alignment_configuration
from onto_merger.analyser import analysis_utils, pandas_profiler
from onto_merger.data.constants import (
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_INPUT,
    DIRECTORY_INTERMEDIATE)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import DataRepository, RuntimeData, convert_runtime_steps_to_named_table, \
    format_datetime
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
        self._alignment_priority_order: List[str] = []
        self._runtime_data: List[RuntimeData] = []

    def run_alignment_and_connection_process(self) -> None:
        """Run the alignment and connectivity process, validate inputs and outputs, produce analysis.

        :return:
        """
        self.logger.info("Started running alignment and connection process for " + f"'{self._short_project_name}'")

        # (1) VALIDATE CONFIG
        self._validate_alignment_config()

        # (2) LOAD AND CHECK INPUT DATA
        self._process_input_data()

        # (3) RUN ALIGNMENT & POST PROCESSING
        self._align_nodes()
        self._post_process_alignment_output()

        # (4) RUN CONNECTIVITY & POST PROCESSING
        self._connect_nodes()

        # (5) FINALISE OUTPUTS
        self._finalise_outputs()

        # (6) VALIDATE: output data
        self._validate_outputs()

        # (7) PRODUCE REPORT
        self._produce_report()

        self.logger.info("Finished running alignment and connection process for " + f"'{self._short_project_name}'")

    def _validate_alignment_config(self) -> None:
        """Run the alignment configuration JSON schema validator.

        Raises an exception if the config is invalid.

        :return:
        """
        self.logger.info("Started validating alignment config...")
        start_date_time = datetime.now()
        config_json_is_valid = validate_alignment_configuration(alignment_config=self._alignment_config.as_dict)
        if config_json_is_valid is False:
            raise Exception
        self._record_runtime(start_date_time=start_date_time, task_name="VALIDATE CONFIG")
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
            tables=analysis_utils.add_namespace_column_to_loaded_tables(tables=self._data_manager.load_input_tables())
        )

        # profile input tables
        start_date_time = datetime.now()
        pandas_profiler.profile_tables(tables=self._data_repo.get_input_tables(), data_manager=self._data_manager)
        self._record_runtime(start_date_time=start_date_time, task_name="PROFILING input")

        # validate input tables
        start_date_time = datetime.now()
        GERunner(
            alignment_config=self._alignment_config, ge_base_directory=self._data_manager.get_data_tests_path()
        ).run_ge_tests(named_tables=self._data_repo.get_input_tables(), data_origin=DIRECTORY_INPUT)
        self._record_runtime(start_date_time=start_date_time, task_name="VALIDATION input")
        self.logger.info("Finished processing input data.")

    def _align_nodes(self) -> None:
        """Run the alignment process.

        Results (merge table and alignment steps) are stored in the data repository.

        :return:
        """
        self.logger.info("Started aligning nodes...")
        start_date_time = datetime.now()
        alignment_results, source_alignment_order = AlignmentManager(
            alignment_config=self._alignment_config,
            data_repo=self._data_repo,
            data_manager=self._data_manager,
        ).align_nodes()
        self._data_repo.update(tables=alignment_results.get_intermediate_tables())
        self._alignment_priority_order.extend(source_alignment_order)
        self._record_runtime(start_date_time=start_date_time, task_name="ALIGNMENT")
        self.logger.info("Finished aligning nodes.")

    def _post_process_alignment_output(self) -> None:
        """Run the merge aggregation process (merges targets become only canonical IDs).

        Results (aggregated merges) are stored in the data repository.

        :return:
        """
        self.logger.info("Started aggregating merges...")
        start_date_time = datetime.now()
        self._data_repo.update(
            tables=merge_utils.post_process_alignment_results(
                data_repo=self._data_repo,
                alignment_priority_order=self._alignment_priority_order
            )
        )
        self._record_runtime(start_date_time=start_date_time, task_name="ALIGNMENT postprocessing")
        self.logger.info("Finished aggregating merges.")

    def _connect_nodes(self) -> None:
        """Run the connectivity process to produce the domain ontology hierarchy.

        Results (hierarchy edges) are stored in the data repository.

        :return:
        """
        self.logger.info("Started connecting nodes...")
        start_date_time = datetime.now()
        self._data_repo.update(
            tables=hierarchy_utils.connect_nodes(
                alignment_config=self._alignment_config,
                source_alignment_order=self._alignment_priority_order,
                data_repo=self._data_repo,
            )
        )
        self._data_repo.update(
            tables=hierarchy_utils.post_process_connectivity_results(
                data_repo=self._data_repo,
            )
        )
        self._record_runtime(start_date_time=start_date_time, task_name="CONNECTIVITY PROCESS")
        self.logger.info("Finished connecting nodes.")

    def _finalise_outputs(self) -> None:
        """Produce the final merged ontology and pre-processes tables for validation.

        Results are stored in the data repository.

        :return:
        """
        start_date_time = datetime.now()
        self.logger.info("Started finalising outputs...")

        #  add NS to all outputs
        self._data_repo.update(
            tables=analysis_utils.add_namespace_column_to_loaded_tables(
                tables=self._data_repo.get_intermediate_tables())
        )

        # save all outputs
        self._data_manager.save_tables(tables=self._data_repo.get_intermediate_tables())

        # save final tables to domain ontology folder
        domain_tables = self._data_manager.produce_domain_ontology_tables(data_repo=self._data_repo)
        self._data_repo.update(tables=domain_tables)
        self._data_manager.save_domain_ontology_tables(tables=domain_tables)

        self.logger.info("Finished finalising outputs.")
        self._record_runtime(start_date_time=start_date_time, task_name="FINALISING OUTPUTS")

    def _validate_outputs(self) -> None:
        """Run the output data validation process.

        :return:
        """
        self.logger.info("Started validating produced data...")

        # run data tests
        start_date_time = datetime.now()
        GERunner(
            alignment_config=self._alignment_config,
            ge_base_directory=self._data_manager.get_data_tests_path(),
        ).run_ge_tests(named_tables=self._data_repo.get_intermediate_tables(), data_origin=DIRECTORY_INTERMEDIATE)
        self._record_runtime(start_date_time=start_date_time, task_name="VALIDATION intermediate")

        start_date_time = datetime.now()
        GERunner(
            alignment_config=self._alignment_config,
            ge_base_directory=self._data_manager.get_data_tests_path(),
        ).run_ge_tests(named_tables=self._data_repo.get_domain_tables(), data_origin=DIRECTORY_DOMAIN_ONTOLOGY)
        self._record_runtime(start_date_time=start_date_time, task_name="VALIDATION output")

        # move data docs to report folder
        self._data_manager.move_data_docs_to_reports()
        self.logger.info("Finished validating produced data.")

    def _produce_report(self) -> None:
        """Run the alignment and connectivity evaluation process.

        :return:
        """
        start_date_time = datetime.now()
        # profile outputs
        pandas_profiler.profile_tables(
            tables=self._data_repo.get_intermediate_tables(), data_manager=self._data_manager
        )
        self._record_runtime(start_date_time=start_date_time, task_name="PROFILING INTERMEDIATE DATA")
        self._data_manager.save_table(
            table=convert_runtime_steps_to_named_table(steps=self._runtime_data)
        )

        # produce HTML report
        # report_path = MergedOntologyAnalyser(
        #     data_repo=self._data_repo, data_manager=self._data_manager
        # ).produce_report()
        # self.logger.info(f"Saved report to {report_path}.")

    def _record_runtime(self, start_date_time: datetime, task_name: str) -> None:
        end_date_time = datetime.now()
        self._runtime_data.append(
            RuntimeData(
                task=task_name,
                start=format_datetime(start_date_time),
                end=format_datetime(end_date_time),
                elapsed=(end_date_time - start_date_time).total_seconds()
            )
        )
