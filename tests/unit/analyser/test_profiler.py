"""Tests for the Profiler class."""
import os

from pandas_profiling import ProfileReport

from onto_merger.analyser import pandas_profiler
from onto_merger.data.constants import TABLES_INPUT
from onto_merger.data.data_manager import DataManager
from tests.fixtures import data_manager


def test_profile_tables(data_manager: DataManager):
    pandas_profiler.profile_tables(tables=data_manager.load_input_tables()[0:2], data_manager=data_manager)

    for table_name in TABLES_INPUT[0:2]:
        report_path = data_manager.get_profiled_table_report_path(table_name=table_name)
        assert os.path.exists(report_path)
        assert os.path.isfile(report_path)
        assert os.stat(report_path).st_size > 100


def test_produce_table_report(data_manager: DataManager):
    actual = pandas_profiler.produce_table_report(table=data_manager.load_input_tables()[0])
    assert isinstance(actual, ProfileReport)
