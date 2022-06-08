"""Runs the alignment and connection process, input and output validation and produces reports."""
from datetime import datetime
from typing import List

from pandas import DataFrame

from onto_merger.alignment import hierarchy_utils, merge_utils
from onto_merger.alignment.alignment_manager import AlignmentManager
from onto_merger.alignment.hierarchy_utils import HierarchyManager
from onto_merger.alignment_config.validator import validate_alignment_configuration
from onto_merger.analyser import analysis_utils, pandas_profiler
from onto_merger.analyser.report_analyser import ReportAnalyser
from onto_merger.data.constants import (
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_INPUT,
    DIRECTORY_INTERMEDIATE,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import (
    DataRepository,
    NamedTable,
    RuntimeData,
    convert_runtime_steps_to_named_table,
    format_datetime,
)
from onto_merger.data_testing.ge_runner import GERunner
from onto_merger.logger.log import setup_logger
from onto_merger.report import report_generator


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

        # (6) VALIDATE & PROFILE: intermediate & output data
        self._validate_and_profile_dataset(
            data_origin=DIRECTORY_INTERMEDIATE,
            data_runtime_name=DIRECTORY_INTERMEDIATE,
            tables=self._data_repo.get_intermediate_tables()
        )
        self._validate_and_profile_dataset(
            data_origin=DIRECTORY_DOMAIN_ONTOLOGY,
            data_runtime_name="output",
            tables=self._data_repo.get_domain_tables()
        )

        # (7) PRODUCE ANALYSIS & REPORT
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
        results_df = self._validate_and_profile_dataset(
            data_origin=DIRECTORY_INPUT,
            data_runtime_name=DIRECTORY_INPUT,
            tables=self._data_repo.get_input_tables()
        )
        errors = results_df["nb_failed_validations"].sum()
        if errors > 0:
            self.logger.error(f"The INPUT data validation found {errors} errors. Terminating process. "
                              + "Please resolve the errors, or force skipping errors in the config (see report "
                              + f"'{self._data_manager.get_ge_data_docs_index_path_for_input()}').")
            if self._alignment_config.base_config.force_through_failed_validation is False:
                raise Exception
            else:
                self.logger.info("Process will carry on due 'force_through_failed_validation' is ON")

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
        self._data_manager.save_tables(tables=alignment_results.get_intermediate_tables())
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
        tables = merge_utils.post_process_alignment_results(
            data_repo=self._data_repo,
            seed_id=self._alignment_config.base_config.seed_ontology_name,
            alignment_priority_order=self._alignment_priority_order
        )
        self._data_repo.update(tables=tables)
        self._data_manager.save_tables(tables=tables)
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
            tables=HierarchyManager(data_manager=self._data_manager).connect_nodes(
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
        self._record_runtime(start_date_time=start_date_time, task_name="CONNECTIVITY")
        self.logger.info("Finished connecting nodes.")

    def _finalise_outputs(self) -> None:
        """Produce the final merged ontology and pre-processes tables for validation.

        Results are stored in the data repository.

        :return:
        """
        self.logger.info("Started finalising outputs...")
        start_date_time = datetime.now()

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

        self._record_runtime(start_date_time=start_date_time, task_name="FINALISING OUTPUTS")
        self.logger.info("Finished finalising outputs.")

    def _validate_and_profile_dataset(
            self, data_origin: str, data_runtime_name: str, tables: List[NamedTable]
    ) -> DataFrame:
        """Profile and validate a dataset.

        :return:
        """
        self.logger.info(f"Started validating {data_runtime_name} data...")

        # profile outputs
        start_date_time = datetime.now()
        pandas_profiler.profile_tables(tables=tables, data_manager=self._data_manager)
        self._record_runtime(start_date_time=start_date_time, task_name=f"PROFILING {data_runtime_name} DATA")

        # run data tests
        start_date_time = datetime.now()
        results_df = GERunner(
            alignment_config=self._alignment_config,
            ge_base_directory=self._data_manager.get_data_tests_path(),
            data_manager=self._data_manager,
        ).run_ge_tests(named_tables=tables, data_origin=data_origin)
        self._record_runtime(start_date_time=start_date_time, task_name=f"VALIDATION {data_runtime_name} DATA")

        self.logger.info(f"Finished validating {data_runtime_name} data.")
        return results_df

    def _produce_report(self) -> None:
        """Run the alignment and connectivity evaluation process.

        :return:
        """
        self.logger.info("Started creating report....")
        run_time_table = convert_runtime_steps_to_named_table(steps=self._runtime_data)
        self._data_repo.update(table=run_time_table)
        self._data_manager.save_table(table=run_time_table)

        # move data docs to report folder
        self._data_manager.move_data_docs_to_reports()

        # run analysis & produce report
        ReportAnalyser(
            alignment_config=self._alignment_config,
            data_repo=self._data_repo,
            data_manager=self._data_manager,
            runtime_data=self._runtime_data
        ).produce_report_data()
        report_path = report_generator.produce_report(data_manager=self._data_manager)

        self.logger.info(f"Finished producing HTML report (saved to '{report_path}'.")

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
