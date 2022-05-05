"""Tests for the Logger."""
import logging
import os
from logging import Logger

from onto_merger.logger.log import (
    add_logger_file,
    get_formatter,
    get_logger,
    setup_logger,
)
from tests.fixtures import TEST_FOLDER_PATH


def test_setup_logger():
    actual = setup_logger(logger_name="FOO")
    assert isinstance(actual, Logger)


def test_get_formatter():
    actual = get_formatter()
    assert isinstance(actual, logging.Formatter)


def test_add_file():
    logger = setup_logger()
    log_file_path = os.path.join(TEST_FOLDER_PATH, "output/onto_merger.log")
    add_logger_file(logger=logger, file_name=log_file_path)

    # check file exists
    assert os.path.isfile(log_file_path) is True
    assert os.stat(log_file_path).st_size == 0

    # write something
    logger.info("test")
    assert os.stat(log_file_path).st_size > 0

    # cleanup
    os.remove(log_file_path)
    assert os.path.isfile(log_file_path) is False


def test_get_logger():
    actual = get_logger(module_name="foobar")
    assert isinstance(actual, Logger)
