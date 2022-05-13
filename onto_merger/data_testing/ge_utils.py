"""Helper methods for creating and configuring a GE data test context."""

from typing import List

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)


def produce_ge_context(ge_base_directory: str) -> BaseDataContext:
    """Produce the GE context configured with the output directory path.

    :param ge_base_directory: The output directory for GE files.
    :return: The context.
    """
    context = BaseDataContext(
        project_config=DataContextConfig(
            store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=ge_base_directory)
        )
    )
    return context


def produce_datasource_config_for_entity(entity_name: str, ge_base_directory: str, data_origin: str) -> dict:
    """Produce a datasource_config dictionary for a given table.

    :param data_origin: The origin of the tested data (INPUT|INTERMEDIATE|DOMAIN_ONTOLOGY).
    :param entity_name: The name of the table that is being tested.
    :param ge_base_directory: The base directory of the GE data test outputs.
    :return: The datasource_config dictionary.
    """
    datasource_config = {
        "name": produce_datasource_name_for_entity(entity_name=entity_name),
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": ge_base_directory,
                "default_regex": {
                    "group_names": [
                        produce_data_asset_name_for_entity(entity_name=entity_name, data_origin=data_origin)
                    ],
                    "pattern": "(.*)",
                },
            },
        },
    }
    return datasource_config


def produce_validation_config_for_entity(entity_name: str, data_origin: str) -> dict:
    """Produce a validation_config dictionary for a given table.

    :param data_origin: The origin of the tested data (INPUT|INTERMEDIATE|DOMAIN_ONTOLOGY).
    :param entity_name: The name of the table that is being tested.
    :return: The validation_config dictionary.
    """
    validation_config = {
        "batch_request": {
            "datasource_name": produce_datasource_name_for_entity(entity_name=entity_name),
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": produce_data_asset_name_for_entity(entity_name=entity_name, data_origin=data_origin),
        },
        "expectation_suite_name": produce_expectation_suite_name_for_entity(entity_name=entity_name),
    }
    return validation_config


def produce_check_point_config(checkpoint_name: str, validations: List[dict]) -> dict:
    """Produce a validation check point config dictionary for a list of validations.

    :param checkpoint_name: The name of the validation checkpoint.
    :param validations: The list of validations.
    :return: The validation check point config dictionary.
    """
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": validations,
    }
    return checkpoint_config


def produce_datasource_name_for_entity(entity_name: str) -> str:
    """Produce a datasource ID for a given table name.

    :param entity_name: The table name.
    :return: The datasource ID.
    """
    datasource_name = f"{entity_name}_datasource"
    return datasource_name


def produce_data_asset_name_for_entity(entity_name: str, data_origin: str) -> str:
    """Produce a data asset ID for a given table name.

    :param data_origin: The origin of the tested data (INPUT|INTERMEDIATE|DOMAIN_ONTOLOGY).
    :param entity_name: The table name.
    :return: The data asset ID.
    """
    data_asset_name = f"{data_origin}_{entity_name}_data_asset"
    return data_asset_name


def produce_expectation_suite_name_for_entity(entity_name: str) -> str:
    """Produce a expectation suite ID for a given table name.

    :param entity_name: The table name.
    :return: The expectation suite ID.
    """
    expectation_suite_name = f"{entity_name}_table"
    return expectation_suite_name
