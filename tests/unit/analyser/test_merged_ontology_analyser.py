import os

import pandas as pd

from onto_merger.analyser.merged_ontology_analyser import MergedOntologyAnalyser
from onto_merger.data.constants import (
    SCHEMA_ALIGNMENT_STEPS_TABLE,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_NODES,
    TABLE_NODES_CONNECTED_ONLY,
    TABLE_NODES_DANGLING,
    TABLE_NODES_MERGED,
    TABLE_CONNECTIVITY_STEPS_REPORT, SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import DataRepository, NamedTable
from tests.fixtures import data_manager, data_repo


def test_produce_report(data_repo: DataRepository, data_manager: DataManager):
    data_repo_copy = DataRepository()
    data_repo_copy.update(
        tables=[
            data_repo.get(table_name=TABLE_NODES),
            NamedTable(TABLE_NODES_MERGED, data_repo.get(table_name=TABLE_NODES).dataframe),
            NamedTable(
                TABLE_NODES_CONNECTED_ONLY,
                data_repo.get(table_name=TABLE_NODES).dataframe,
            ),
            NamedTable(TABLE_NODES_DANGLING, data_repo.get(table_name=TABLE_NODES).dataframe),
            NamedTable(
                TABLE_ALIGNMENT_STEPS_REPORT,
                pd.DataFrame([], columns=SCHEMA_ALIGNMENT_STEPS_TABLE),
            ),
            NamedTable(
                TABLE_CONNECTIVITY_STEPS_REPORT,
                pd.DataFrame([], columns=SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE),
            ),
        ]
    )

    print(data_repo_copy)
    actual_file_path = MergedOntologyAnalyser(data_repo=data_repo_copy, data_manager=data_manager).produce_report()
    expected_file_path = data_manager.produce_merged_ontology_report_path()
    assert isinstance(actual_file_path, str)
    assert actual_file_path == expected_file_path
    assert os.path.exists(actual_file_path)
    assert os.path.isfile(actual_file_path) is True
    assert os.stat(actual_file_path).st_size > 100
