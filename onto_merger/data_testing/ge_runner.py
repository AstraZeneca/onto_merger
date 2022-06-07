"""Setup and run data testing for a parsed ontology."""
import logging
import os
import sys
from typing import List, Union

from great_expectations.core import ExpectationSuite
from pandas import DataFrame
from ruamel import yaml

from onto_merger.analyser.report_analyser_utils import (
    produce_ge_validation_analysis_as_table,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import AlignmentConfig, NamedTable
from onto_merger.data_testing.ge_expectation_helper import (
    produce_expectations_for_table,
)
from onto_merger.data_testing.ge_utils import (
    produce_check_point_config,
    produce_datasource_config_for_entity,
    produce_expectation_suite_name_for_entity,
    produce_ge_context,
    produce_validation_config_for_entity,
)

logger = logging.getLogger(__name__)


class GERunner:
    """Runs data tests for the input & output tables and produces test documentation.

    Although the parser is tested and will produce standardised output,
    the parsed data might have some bugs. Data tests uncover these issues.
    Some of these can be spotted in the Pandas profiling output,
    but data tests automate manual inspection.

    Data tests are run using the Great Expectations data test framework.
    """

    checkpoint_name = "first_checkpoint"

    def __init__(self, alignment_config: AlignmentConfig, ge_base_directory: str, data_manager: DataManager) -> None:
        """Initialise the class.

        :param alignment_config: The alignment process configuration dataclass.
        :param ge_base_directory: The base directory of the validation framework.
        :param data_manager: The data manager instance.
        be stored.
        """
        self._alignment_config = alignment_config
        self._ge_base_directory = ge_base_directory
        self._ge_context = produce_ge_context(ge_base_directory=self._ge_base_directory)
        self._data_manager = data_manager

    def run_ge_tests(self, named_tables: List[NamedTable], data_origin: str) -> Union[DataFrame, None]:
        """Run data tests for a list of named tables.

        :param data_origin: The origin of the tested data (INPUT|INTERMEDIATE|DOMAIN_ONTOLOGY).
        :param named_tables: The list of named tables.
        :return:
        """
        # disable print to console (by GE framework)
        block_print()
        if len(named_tables) == 0:
            enable_print()
            return None
        logger.info("Started Great Expectations data tests...")

        # for table, i.e. nodes edges and mappings
        # add to the GE context: the data source, expectation suite and the expectations
        self._configure_ge_context_data_sources(loaded_tables=named_tables, data_origin=data_origin)

        # configure expectations suites with expectations (data tests)
        self._configure_ge_expectation_suites_for_entity(loaded_tables=named_tables)

        # create a checkpoint with validations (each validation links exp suite to a
        # datasource) run tests (via checkpoint)
        self._run_validations(loaded_tables=named_tables, data_origin=data_origin)

        # produce the data docs
        self._ge_context.build_data_docs()

        # aggregate results
        results_df = produce_ge_validation_analysis_as_table(data_manager=self._data_manager)

        # done
        logger.info("Finished running Great Expectations data tests.")
        enable_print()
        return results_df

    def _configure_ge_context_data_sources(self, loaded_tables: List[NamedTable], data_origin: str) -> None:
        """Update the data test context with the tables that are being tested.

        :param data_origin: The origin of the tested data (INPUT|INTERMEDIATE|DOMAIN_ONTOLOGY).
        :param loaded_tables: The list of named tables.
        :return:
        """
        for loaded_table in loaded_tables:
            datasource_config = produce_datasource_config_for_entity(
                entity_name=loaded_table.name, ge_base_directory=self._ge_base_directory, data_origin=data_origin
            )
            # add the data source
            self._ge_context.add_datasource(**datasource_config)

            # save the context
            self._ge_context.test_yaml_config(yaml.dump(datasource_config))

    def _configure_ge_expectation_suites_for_entity(self, loaded_tables: List[NamedTable]) -> None:
        """Configure the data test expectation suites with tables that are being tested.

        :param loaded_tables: The list of tables being tested.
        :return:
        """
        # create a suite per entity (parsed table)
        for loaded_table in loaded_tables:
            suite = self._produce_expectation_suite_for_entity(entity_name=loaded_table.name)

            # get the expectations for the entity and add each to the suite
            expectation_configurations_for_entity = produce_expectations_for_table(
                table_name=loaded_table.name,
                alignment_config=self._alignment_config,
            )
            for expectation_configuration in expectation_configurations_for_entity:
                suite.add_expectation(expectation_configuration=expectation_configuration)

            # save the suite
            self._ge_context.save_expectation_suite(expectation_suite=suite)

    def _produce_expectation_suite_for_entity(self, entity_name: str) -> ExpectationSuite:
        """Produce an expectation suite configuration (set of data tests) for a given table.

        :param entity_name: The name of the table.
        :return: The configured expectation suite.
        """
        suite = self._ge_context.create_expectation_suite(
            expectation_suite_name=produce_expectation_suite_name_for_entity(entity_name=entity_name),
            overwrite_existing=True,
        )
        return suite

    def _run_validations(self, loaded_tables: List[NamedTable], data_origin: str) -> None:
        """Run the data tests for a list of tables.

        :param data_origin: The origin of the tested data (INPUT|INTERMEDIATE|DOMAIN_ONTOLOGY).
        :param loaded_tables: The list of tables being tested.
        :return:
        """
        for loaded_table in loaded_tables:
            # create the checkpoint
            self._ge_context.add_checkpoint(
                **produce_check_point_config(
                    checkpoint_name=self.checkpoint_name,
                    validations=[
                        produce_validation_config_for_entity(entity_name=loaded_table.name, data_origin=data_origin)
                    ],
                )
            )

            # run the checkpoint todo aggregate and print results
            self._ge_context.run_checkpoint(
                checkpoint_name=self.checkpoint_name,
                batch_request={
                    "runtime_parameters": {"batch_data": loaded_table.dataframe},
                    "batch_identifiers": {"default_identifier_name": "default_identifier_name"},
                },
            )


def block_print() -> None:
    """Block outputs to the console (GE produces many debug level outputs)."""
    sys.stdout = open(os.devnull, "w")


def enable_print() -> None:
    """Enable outputs to the console."""
    sys.stdout = sys.__stdout__
