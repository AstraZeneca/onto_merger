"""Tests for the data classes."""
from typing import List

import numpy as np
import pandas as pd
from pandas import DataFrame

from onto_merger.data.constants import (
    SCHEMA_ALIGNMENT_STEPS_TABLE,
    SCHEMA_DATA_REPO_SUMMARY,
    SCHEMA_MAPPING_TABLE,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_MAPPINGS,
)
from onto_merger.data.dataclasses import (
    AlignmentConfigMappingTypeGroups,
    AlignmentStep,
    DataRepository,
    NamedTable,
    convert_alignment_steps_to_named_table,
)


def test_alignment_step_dataclass():
    mapping_type_group = "foo"
    source_id = "MONDO"
    step_counter = 1
    count_unmapped_nodes = 100

    actual = AlignmentStep(
        mapping_type_group=mapping_type_group,
        source=source_id,
        step_counter=step_counter,
        count_unmapped_nodes=count_unmapped_nodes,
    )

    assert actual.mapping_type_group == mapping_type_group
    assert actual.source == source_id
    assert actual.step_counter == step_counter
    assert actual.count_unmapped_nodes == count_unmapped_nodes
    assert actual.count_mappings == 0
    assert actual.count_nodes_one_source_to_many_target == 0
    assert actual.count_merged_nodes == 0


def test_alignment_config_mapping_type_groups_dataclass():
    equivalence = ["foo"]
    database_reference = ["bar"]
    label_match = ["bla"]

    actual = AlignmentConfigMappingTypeGroups(
        equivalence=equivalence,
        database_reference=database_reference,
        label_match=label_match,
    )

    assert actual.equivalence == equivalence
    assert actual.database_reference == database_reference
    assert actual.label_match == label_match
    assert actual.all_mapping_types == (equivalence + database_reference + label_match)


def test_data_repository_dataclass():
    # empty
    data_repo = DataRepository()
    assert data_repo.get_intermediate_tables() == []
    assert data_repo.get_input_tables() == []

    # update table
    table = NamedTable(
        TABLE_MAPPINGS,
        pd.DataFrame(
            [
                ("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO"),
                ("MONDO:0000005", "MONDO:0000456", "equivalent_to", "MONDO"),
            ],
            columns=SCHEMA_MAPPING_TABLE,
        ),
    )
    data_repo.update(table=table)
    result = data_repo.get_input_tables()[0]
    assert isinstance(result, NamedTable)
    assert isinstance(data_repo.get(table_name=TABLE_MAPPINGS), NamedTable)

    # get_repo_summary
    exp_repo_summary = pd.DataFrame(
        [
            (TABLE_MAPPINGS, "2", SCHEMA_MAPPING_TABLE),
        ],
        columns=SCHEMA_DATA_REPO_SUMMARY,
    )
    result_repo_summary = data_repo.get_repo_summary()
    print(result_repo_summary)
    print(exp_repo_summary)
    assert isinstance(result_repo_summary, DataFrame)
    assert np.array_equal(result_repo_summary.values, exp_repo_summary.values) is True


def test_convert_alignment_steps_to_named_table():
    SCHEMA_NO_DATES: List[str] = SCHEMA_ALIGNMENT_STEPS_TABLE[0:8]

    input_data = [
        AlignmentStep(
            mapping_type_group="eqv",
            source="FOO",
            step_counter=1,
            count_unmapped_nodes=100,
        ),
        AlignmentStep(
            mapping_type_group="eqv",
            source="BAR",
            step_counter=2,
            count_unmapped_nodes=90,
        ),
    ]
    actual = convert_alignment_steps_to_named_table(alignment_steps=input_data)
    expected = pd.DataFrame(
        [("eqv", "FOO", 1, 100, 0, 0, 0, "Aligning FOO eqv", "2022-06-06 09:37:36", "2022-06-06", "09:37:36.604905", 0),
         ("eqv", "BAR", 2, 90, 0, 0, 0, "Aligning BAR eqv", "2022-06-06 09:37:36", "2022-06-06", "09:37:36.604935", 0)],
        columns=SCHEMA_ALIGNMENT_STEPS_TABLE,
    )
    print(actual.dataframe, "\n\n")
    print(expected, "\n\n")

    assert isinstance(actual, NamedTable)
    assert actual.name == TABLE_ALIGNMENT_STEPS_REPORT
    assert isinstance(actual.dataframe, DataFrame)
    assert np.array_equal(actual.dataframe[SCHEMA_NO_DATES].values, expected[SCHEMA_NO_DATES].values) is True
