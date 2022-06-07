import pytest

from onto_merger.alignment_config.validator import validate_alignment_configuration
from tests.fixtures import data_manager


def test_validate_configuration_fail():
    config = {"foobar": "fizzbang"}

    actual = validate_alignment_configuration(alignment_config=config)

    assert isinstance(actual, bool)
    assert actual is False


def test_validate_configuration_pass(data_manager):
    config = data_manager.load_alignment_config()

    actual = validate_alignment_configuration(alignment_config=config.as_dict)

    assert isinstance(actual, bool)
    assert actual is True
