"""Tests for the mapping_utils."""
import numpy as np
import pandas as pd
from pandas import DataFrame

from onto_merger.alignment import mapping_utils
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_MAPPING_HASH,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    SCHEMA_MAPPING_TABLE,
)


def test_get_mappings_internal_node_reassignment():
    input_mappings = pd.DataFrame(
        [
            ("FOO:0000004", "MONDO:0000123", "equivalent_to", "MONDO"),
            ("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO"),
            ("MONDO:0000005", "MONDO:0000456", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )
    actual = mapping_utils.get_mappings_internal_node_reassignment(mappings=input_mappings)
    data = [
        ("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO"),
        ("MONDO:0000005", "MONDO:0000456", "equivalent_to", "MONDO"),
    ]
    expected = pd.DataFrame(data, columns=SCHEMA_MAPPING_TABLE)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_mappings_obsolete_to_current_node_id():
    input_nodes_obsolete = pd.DataFrame(
        ["MONDO:0000123", "MONDO:0000456"], columns=[COLUMN_DEFAULT_ID])
    input_mappings = pd.DataFrame(
        [
            ("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO", "MONDO", "MONDO"),
            ("MONDO:0000456", "MONDO:0000005",  "equivalent_to", "MONDO", "MONDO", "MONDO"),
        ],
        columns=(SCHEMA_MAPPING_TABLE + ["namespace_source_id", "namespace_target_id"]),
    )
    actual = mapping_utils.get_mappings_obsolete_to_current_node_id(
        nodes_obsolete=input_nodes_obsolete,
        mappings=input_mappings
    )
    data = [
        ("MONDO:0000123", "MONDO:0000004", "equivalent_to", "MONDO"),
        ("MONDO:0000456", "MONDO:0000005", "equivalent_to", "MONDO"),
    ]
    expected = pd.DataFrame(data, columns=SCHEMA_MAPPING_TABLE)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_mappings_for_namespace():
    # input
    input_mappings = pd.DataFrame(
        [
            ("MONDO:0000123", "FOOBAR:0000004", "equivalent_to", "MONDO"),
            ("UMLS:0000005", "SNOMED:0000456", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )

    # run
    actual = mapping_utils.get_mappings_for_namespace(namespace="MONDO", edges=input_mappings)

    # expected
    expected = pd.DataFrame(
        [("MONDO:0000123", "FOOBAR:0000004", "equivalent_to", "MONDO")],
        columns=SCHEMA_MAPPING_TABLE,
    )

    # evaluate
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_one_or_many_source_to_one_target_mappings():
    input_mappings = pd.DataFrame(
        [
            ("FOO:001", "BAR:001", "equivalent_to", "MONDO"),
            ("FOO:001", "BAR:002", "equivalent_to", "MONDO"),
            ("FOO:002", "BAR:003", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )
    actual = mapping_utils.get_one_or_many_source_to_one_target_mappings(mappings=input_mappings)
    expected = pd.DataFrame([("FOO:002", "BAR:003", "equivalent_to", "MONDO")], columns=SCHEMA_MAPPING_TABLE)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_one_source_to_many_target_mappings():
    input_mappings = pd.DataFrame(
        [
            ("FOO:001", "BAR:001", "equivalent_to", "MONDO"),
            ("FOO:001", "BAR:002", "equivalent_to", "MONDO"),
            ("FOO:002", "BAR:003", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )
    actual = mapping_utils.get_one_source_to_many_target_mappings(mappings=input_mappings)
    expected = pd.DataFrame(
        [
            ("FOO:001", "BAR:001", "equivalent_to", "MONDO"),
            ("FOO:001", "BAR:002", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_mappings_with_mapping_relations():
    # input
    input_mappings = pd.DataFrame(
        [
            ("FOO:001", "BAR:001", "equivalent_to", "MONDO"),
            ("FOO:001", "BAR:002", "xref", "MONDO"),
            ("FOO:002", "BAR:003", "bla", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )

    # run
    actual = mapping_utils.get_mappings_with_mapping_relations(
        permitted_mapping_relations=["equivalent_to", "foo"], mappings=input_mappings
    )

    # expected
    expected = pd.DataFrame([("FOO:001", "BAR:001", "equivalent_to", "MONDO")], columns=SCHEMA_MAPPING_TABLE)

    # evaluate
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_orient_mappings_to_namespace():
    # input
    input_mappings = pd.DataFrame(
        [
            ("FOO:001", "BAR:001", "equivalent_to", "MONDO"),
            ("BAR:002", "FOO:001", "xref", "MONDO"),
            ("FOO:002", "BLABLA:003", "bla", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )

    # run
    actual = mapping_utils.orient_mappings_to_namespace(required_target_id_namespace="FOO", mappings=input_mappings)

    # expected
    expected = pd.DataFrame(
        [
            ("BAR:001", "FOO:001", "equivalent_to", "MONDO"),
            ("BAR:002", "FOO:001", "xref", "MONDO"),
            ("BLABLA:003", "FOO:002", "bla", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )

    # evaluate
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_add_comparison_column_for_reoriented_mappings():
    # input
    input_mappings = pd.DataFrame([("FOO:001", "BAR:001", "equivalent_to", "MONDO")], columns=SCHEMA_MAPPING_TABLE)

    # run
    actual = mapping_utils.add_comparison_column_for_reoriented_mappings(mappings=input_mappings)

    # expected
    expected = pd.DataFrame(
        [
            (
                "FOO:001",
                "BAR:001",
                "equivalent_to",
                "MONDO",
                "['BAR:001', 'FOO:001']|equivalent_to|MONDO",
            )
        ],
        columns=(SCHEMA_MAPPING_TABLE + [COLUMN_MAPPING_HASH]),
    )

    # evaluate
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_update_mappings_with_current_node_ids():
    # input
    input_internal_obsolete_to_current_node_id = pd.DataFrame(
        [
            ("MONDO:0000004", "MONDO:0000123", "equivalent_to", "MONDO"),
            ("MONDO:0000005", "MONDO:0000456", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )
    input_mappings = pd.DataFrame(
        [
            ("SNOMED:123", "MONDO:0000004", "equivalent_to", "MONDO"),
            ("FOOBAR:456", "MONDO:0000456", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )

    # run
    actual = mapping_utils.update_mappings_with_current_node_ids(
        mappings_internal_obsolete_to_current_node_id=input_internal_obsolete_to_current_node_id,
        mappings=input_mappings,
    )

    # expected
    expected = pd.DataFrame(
        [
            ("SNOMED:123", "MONDO:0000123", "equivalent_to", "MONDO"),
            ("FOOBAR:456", "MONDO:0000456", "equivalent_to", "MONDO"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )

    # evaluate
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_get_mappings_with_updated_node_ids():
    pass


def test_filter_mappings_for_node_set():
    input_nodes = pd.DataFrame(["SNOMED:001", "FOOBAR:1234"], columns=[COLUMN_DEFAULT_ID])
    input_mappings = pd.DataFrame(
        [
            ("SNOMED:001", "MONDO:0000123", "foo", "TEST"),
            ("SNOMED:002", "MONDO:0000234", "foo", "TEST"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )
    expected = pd.DataFrame(
        [("SNOMED:001", "MONDO:0000123", "foo", "TEST")],
        columns=SCHEMA_MAPPING_TABLE,
    )
    actual = mapping_utils.filter_mappings_for_node_set(nodes=input_nodes, mappings=input_mappings)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_deduplicate_mappings_for_type_group():
    input_mappings = pd.DataFrame(
        [
            ("SNOMED:001", "MONDO:0000123", "foo", "TEST"),
            ("SNOMED:001", "MONDO:0000123", "bar", "TEST"),
        ],
        columns=SCHEMA_MAPPING_TABLE,
    )
    expected = pd.DataFrame(
        [("SNOMED:001", "MONDO:0000123", "eqv", "TEST")],
        columns=SCHEMA_MAPPING_TABLE,
    )
    actual = mapping_utils.deduplicate_mappings_for_type_group(mapping_type_group_name="eqv", mappings=input_mappings)
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True


def test_produce_self_merges_for_seed_nodes():
    # input
    nodes = pd.DataFrame(
        [
            "MONDO:0000001",
            "MONDO:0000002",
            "MONDO:0000003",
            "MONDO:0000004",
            "SNOMED:001",
        ],
        columns=[COLUMN_DEFAULT_ID],
    )
    nodes_obsolete = pd.DataFrame(
        [
            "MONDO:0000001",
        ],
        columns=[COLUMN_DEFAULT_ID],
    )

    # run
    actual = mapping_utils.produce_self_merges_for_seed_nodes(
        seed_id="MONDO", nodes=nodes, nodes_obsolete=nodes_obsolete
    ).dataframe

    # expected
    expected = pd.DataFrame(
        [
            ("MONDO:0000002", "MONDO:0000002"),
            ("MONDO:0000003", "MONDO:0000003"),
            ("MONDO:0000004", "MONDO:0000004"),
        ],
        columns=[COLUMN_SOURCE_ID, COLUMN_TARGET_ID],
    )

    # evaluate
    assert isinstance(actual, DataFrame)
    assert np.array_equal(actual.values, expected.values) is True
