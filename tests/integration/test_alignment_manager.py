"""Tests for the AlignmentManager."""
import os
from typing import List

import pytest

from onto_merger.alignment.alignment_manager import AlignmentManager
from onto_merger.data.constants import (
    DIRECTORY_ANALYSIS,
    DIRECTORY_DATA_TESTS,
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_DROPPED_MAPPINGS,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_REPORT,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_MERGES_WITH_META_DATA,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import AlignmentConfig, DataRepository, NamedTable
from tests.fixtures import (
    TEST_FOLDER_OUTPUT_PATH,
    alignment_config,
    data_manager,
    data_repo,
    source_alignment_priority_order,
)


def test_align_nodes(
    alignment_config: AlignmentConfig,
    data_repo: DataRepository,
    data_manager: DataManager,
    source_alignment_priority_order: List[str],
):
    actual_outputs = set(os.listdir(TEST_FOLDER_OUTPUT_PATH))
    assert actual_outputs == {
        DIRECTORY_REPORT,
        DIRECTORY_INTERMEDIATE,
        DIRECTORY_DOMAIN_ONTOLOGY,
    }
    assert os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_DOMAIN_ONTOLOGY)) == []
    assert set(os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_INTERMEDIATE))) \
           == {DIRECTORY_DATA_TESTS, DIRECTORY_DROPPED_MAPPINGS, DIRECTORY_ANALYSIS}
    assert os.listdir(os.path.join(TEST_FOLDER_OUTPUT_PATH, DIRECTORY_INTERMEDIATE, DIRECTORY_DROPPED_MAPPINGS)) \
           == []

    output_data_repo, source_alignment_order = AlignmentManager(
        alignment_config=alignment_config,
        data_repo=data_repo,
        data_manager=data_manager,
    ).align_nodes()

    expected_output_named_tables = [TABLE_ALIGNMENT_STEPS_REPORT, TABLE_MERGES_WITH_META_DATA]
    for output_table_name in expected_output_named_tables:
        actual = output_data_repo.get(output_table_name)
        assert isinstance(actual, NamedTable)
        assert len(actual.dataframe) > 0
    assert source_alignment_order == source_alignment_priority_order
    assert isinstance(output_data_repo, DataRepository)
