"""Validate the alignment configuration JSON schema."""

import logging

from jsonschema import Draft7Validator

from onto_merger.alignment_config.json_schema import schema
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def validate_alignment_configuration(alignment_config: dict) -> bool:
    """
    Validate the alignment configuration JSON schema.

    :param alignment_config: The configuration JSON as a dictionary.
    :return: True is valid, otherwise False.
    """
    logging.info("Validating parser configuration...")

    # perform check
    schema_validation_errors = [
        error
        for error in sorted(
            Draft7Validator(schema).iter_errors(alignment_config),
            key=lambda e: e.path,
        )
    ]
    check_passed = True if len(schema_validation_errors) == 0 else False

    # log any errors
    [logger.error(f"Alignment config error: {error}") for error in schema_validation_errors]
    logging.info(
        f"Validation is complete, result: {check_passed} (there are {len(schema_validation_errors)} error(s))."
    )

    return check_passed
