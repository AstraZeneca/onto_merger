"""Logger configuration."""

import logging
import sys
from logging import Logger
from typing import Any

APP_NAME = "OntoMerger"


def setup_logger(module_name: str, file_name: str, logger_name=APP_NAME, is_debug=False) -> Logger:
    """Produce and configure the project logger with the output stream, formatting and log level.

    :param file_name: The log output file path.
    :param module_name: The module name.
    :param logger_name: The project logger name.
    :param is_debug: Set the logger level to debug or info.
    :return: The logger.
    """
    logger = logging.getLogger(logger_name)
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG if is_debug else logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(hdlr=sh)

    fh = logging.FileHandler(file_name)
    fh.setFormatter(fmt=formatter)
    logger.addHandler(hdlr=fh)

    return logger.getChild(module_name)


def get_logger(module_name) -> Any:
    """Produce the project logger for a module.

    :param module_name: The module name.
    :return: The logger.
    """
    return logging.getLogger(APP_NAME).getChild(module_name)
