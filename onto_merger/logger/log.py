import logging
import sys
from logging import Logger
from typing import Any

APP_NAME = "OntoMerger"


def setup_logger(logger_name=APP_NAME, is_debug=True) -> Logger:
    """Produces and configures the project logger with the output stream,
    formatting and log level.

    :param logger_name: The project logger name.
    :param is_debug:
    :return: The logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

    formatter = get_formatter()

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(sh)

    return logger


def get_formatter() -> logging.Formatter:
    """Configures the logger formatter.

    :return: The formatter.
    """
    return logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def add_logger_file(logger: Logger, file_name=str) -> None:
    """Sets the log output file path.

    :param logger: The logger.
    :param file_name: The file path.
    :return:
    """
    logger.addHandler(hdlr=logging.FileHandler(file_name))


def get_logger(module_name) -> Any:
    """Produces the project logger for a module.

    :param module_name: The module name.
    :return: The logger.
    """
    return logging.getLogger(APP_NAME).getChild(module_name)
