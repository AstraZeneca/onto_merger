"""Tests for the GE utils methods."""
from great_expectations.data_context import BaseDataContext

from onto_merger.data_testing.ge_utils import (
    produce_check_point_config,
    produce_data_asset_name_for_entity,
    produce_datasource_config_for_entity,
    produce_datasource_name_for_entity,
    produce_expectation_suite_name_for_entity,
    produce_ge_context,
    produce_validation_config_for_entity,
)
from tests.fixtures import ge_test_folder_path


def test_produce_ge_context(ge_test_folder_path):
    actual = produce_ge_context(ge_base_directory=ge_test_folder_path)
    assert isinstance(actual, BaseDataContext)


def test_produce_datasource_config_for_entity():
    expected = {
        "name": "bar_datasource",
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
                "base_directory": "foo",
                "default_regex": {
                    "group_names": ["INPUT_bar_data_asset"],
                    "pattern": "(.*)",
                },
            },
        },
    }
    actual = produce_datasource_config_for_entity(entity_name="bar", ge_base_directory="foo", data_origin="INPUT")
    assert isinstance(actual, dict)
    assert actual == expected


def test_produce_validation_config_for_entity():
    expected = {
        "batch_request": {
            "datasource_name": "foo_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "OUTPUT_foo_data_asset",
        },
        "expectation_suite_name": "foo_table",
    }
    actual = produce_validation_config_for_entity(entity_name="foo", data_origin="OUTPUT")
    assert isinstance(actual, dict)
    assert actual == expected


def test_produce_check_point_config():
    expected = {
        "name": "foo",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [{"foo": "bar"}],
    }
    actual = produce_check_point_config(checkpoint_name="foo", validations=[{"foo": "bar"}])
    assert isinstance(actual, dict)
    assert actual == expected


def test_produce_datasource_name_for_entity():
    actual = produce_datasource_name_for_entity(entity_name="foo")
    assert isinstance(actual, str)
    assert actual == "foo_datasource"


def test_produce_data_asset_name_for_entity():
    actual = produce_data_asset_name_for_entity(entity_name="foo", data_origin="bla")
    assert isinstance(actual, str)
    assert actual == "bla_foo_data_asset"


def test_produce_expectation_suite_name_for_entity():
    actual = produce_expectation_suite_name_for_entity(entity_name="foo")
    assert isinstance(actual, str)
    assert actual == "foo_table"
