"""GE Expectation configuration helper methods."""

from typing import List

from great_expectations.core import ExpectationConfiguration

from onto_merger.analyser.analysis_util import get_namespace_column_name_for_column
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    EDGE,
    MAPPING,
    MERGE,
    RDFS_SUBCLASS_OF,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MAPPINGS,
    TABLE_MERGES,
    TABLE_NODES,
    TABLE_NODES_OBSOLETE,
    TABLE_NODES_UNMAPPED,
)
from onto_merger.data.dataclasses import AlignmentConfig


def produce_expectations_for_table(
    table_name: str, alignment_config: AlignmentConfig
) -> List[ExpectationConfiguration]:
    """Produce the  list of relevant ExpectationConfiguration-s for a given table (node or edge).

    :param table_name: The table name.
    :param alignment_config: The alignment process configuration dataclass.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    if table_name in [TABLE_NODES, TABLE_NODES_OBSOLETE, TABLE_NODES_UNMAPPED]:
        return produce_node_table_expectations()
    elif table_name in [TABLE_EDGES_HIERARCHY, TABLE_EDGES_HIERARCHY_POST]:
        return produce_edge_table_expectations(main_edge_type=EDGE, alignment_config=alignment_config)
    elif table_name in [TABLE_MAPPINGS]:
        return produce_edge_table_expectations(main_edge_type=MAPPING, alignment_config=alignment_config)
    elif table_name in [TABLE_MERGES]:
        return produce_edge_table_expectations(main_edge_type=MERGE, alignment_config=alignment_config)
    else:
        return []


def produce_node_table_expectations() -> List[ExpectationConfiguration]:
    """Produce the  list of relevant ExpectationConfiguration-s for a given node table.

    :return: The list of relevant ExpectationConfiguration-s.
    """
    expectations = [
        produce_expectation_config_expect_table_columns_to_match_set(
            column_set=[COLUMN_DEFAULT_ID, get_namespace_column_name_for_column(COLUMN_DEFAULT_ID)]
        )
    ]
    expectations.extend(produce_node_short_id_expectations(column_name=COLUMN_DEFAULT_ID, is_node_table=True))
    return expectations


def produce_edge_table_expectations(
    main_edge_type: str, alignment_config: AlignmentConfig
) -> List[ExpectationConfiguration]:
    """Produce the list of ExpectationConfiguration-s for an edge table (mapping, merge, hierarchy).

    :param main_edge_type: The edge table type (hierarchy, mapping, merge).
    :param alignment_config: The alignment process configuration dataclass.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    return (
        [
            produce_expectation_config_expect_table_columns_to_match_set(
                column_set=get_column_set_for_edge_table(main_edge_type=main_edge_type)
            )
        ]
        + produce_node_short_id_expectations(column_name=COLUMN_SOURCE_ID, is_node_table=False)
        + produce_node_short_id_expectations(column_name=COLUMN_TARGET_ID, is_node_table=False)
        + []
        if main_edge_type == MERGE
        else produce_edge_relation_expectations(
            column_name=COLUMN_RELATION,
            edge_types=get_edge_types_for_edge_table(main_edge_type=main_edge_type, alignment_config=alignment_config),
        )
    )


def get_column_set_for_edge_table(main_edge_type: str) -> List[str]:
    """Return the column names (table schema) for a given edge table.

    :param main_edge_type: The edge table type (hierarchy, mapping, merge).
    :return: The list of columns for the given table.
    """
    column_set_merges = [
        COLUMN_SOURCE_ID,
        COLUMN_TARGET_ID,
        get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
        get_namespace_column_name_for_column(COLUMN_TARGET_ID),
    ]
    column_set_mappings = column_set_merges + [
        COLUMN_RELATION,
        COLUMN_PROVENANCE,
    ]
    if main_edge_type == EDGE:
        return column_set_mappings
    elif main_edge_type == MAPPING:
        return column_set_mappings
    elif main_edge_type == MERGE:
        return column_set_merges
    return []


def get_edge_types_for_edge_table(main_edge_type: str, alignment_config: AlignmentConfig) -> List[str]:
    """Produce a list of edge types that are permitted in the given edge table.

    Edge hierarchy table can only have 'rdfs:subClassOf' relation, whereas a mapping table
    can have different edge, i.e. mapping relation, types as specified in the
    AlignmentConfig.

    :param main_edge_type: The edge table type (hierarchy, mapping, merge).
    :param alignment_config: The alignment process configuration dataclass.
    :return: The list of permitted edge types for the given table.
    """
    if main_edge_type == EDGE:
        return [RDFS_SUBCLASS_OF]
    elif main_edge_type == MAPPING:
        return alignment_config.mapping_type_groups.all_mapping_types
    return []


def produce_node_short_id_expectations(column_name: str, is_node_table: bool) -> List[ExpectationConfiguration]:
    """Produce the list of ExpectationConfiguration-s for a column that represents a node ID.

    Node IDs occur in the node tables (where the ID must be unique), but also in edge
    tables (source_id, target_id, where the ID may be repeated).

    :param column_name: The column that contains the node ID.
    :param is_node_table: If true, the ID column will be checked for uniqueness;
    otherwise repeated IDs are permitted in the column.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    expectations = [
        produce_expectation_config_expect_column_to_exist(column_name=column_name),
        produce_expectation_config_column_type_string(column_name=column_name),
        produce_expectation_config_column_values_to_not_be_null(column_name=column_name),
        produce_expectation_config_column_value_lengths_to_be_between(
            column_name=column_name, min_value=3, max_value=25
        ),
        produce_expectation_config_column_values_to_match_regex(column_name=column_name, regex="^[A-Za-z][\\w]*:\\S+$"),
    ]
    # node IDs must be unique in the node table, but not in edges and mappings
    if is_node_table:
        expectations.append(produce_expectation_config_column_values_to_be_unique(column_name=column_name))
    return expectations


def produce_edge_relation_expectations(column_name: str, edge_types: List[str]) -> List[ExpectationConfiguration]:
    """Produce the list of ExpectationConfiguration-s for a column that represents an edge relation (type).

    :param column_name: The column contains the edge relation.
    :param edge_types: The list of permitted edge relation types.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    return [
        produce_expectation_config_expect_column_to_exist(column_name=column_name),
        produce_expectation_config_column_type_string(column_name=column_name),
        produce_expectation_config_column_values_to_not_be_null(column_name=column_name),
        produce_expectation_config_expect_column_values_to_be_in_set(column_name=column_name, value_set=edge_types),
        produce_expectation_config_column_value_lengths_to_be_between(
            column_name=column_name, min_value=3, max_value=30
        ),
    ]


def produce_expectation_config_column_type_string(column_name: str,) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration for a given string column to check that it only has string type objects.

    :param column_name: The name of the column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": column_name, "type_": "object", "mostly": 1.00},
        meta=None,
    )


def produce_expectation_config_column_values_to_not_be_null(column_name: str,) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration for a given column to check whether it contains any null values.

    :param column_name: The name of the column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": column_name, "mostly": 1.00},
        meta=None,
    )


def produce_expectation_config_column_values_to_be_unique(column_name: str,) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration to check whether a given column has only unique values.

    :param column_name: The name of the column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique", kwargs={"column": column_name, "mostly": 1.00}, meta=None,
    )


def produce_expectation_config_column_value_lengths_to_be_between(
    column_name: str, min_value: int, max_value: int
) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration where we expect column entries to be strings.

    :param column_name: The name of the table column to be tested with this expectation.
    :param min_value: The min length.
    :param max_value: The max length.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_value_lengths_to_be_between",
        kwargs={"column": column_name, "min_value": min_value, "max_value": max_value, "mostly": 1.00},
        meta=None,
    )


def produce_expectation_config_column_values_to_match_regex(column_name: str, regex: str) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration for a given string column to match a regex.

    :param regex: The regex to be matched.
    :param column_name: The name of the table column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_regex",
        kwargs={"column": column_name, "regex": regex, "mostly": 1.00},
        meta=None,
    )


def produce_expectation_config_expect_column_to_exist(column_name: str,) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration to check the existence a given column in a table.

    :param column_name: The name of the table column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_to_exist", kwargs={"column": column_name}, meta=None,
    )


def produce_expectation_config_expect_column_values_to_be_in_set(
    column_name: str, value_set: List[str]
) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration to check whether a given column has only the specified values.

    :param value_set: The permitted values for the column.
    :param column_name: The name of the table column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": column_name, "value_set": value_set, "mostly": 1.00},
        meta=None,
    )


def produce_expectation_config_expect_table_columns_to_match_set(column_set: List[str],) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration to check whether a given table has only the specified columns.

    :param column_set: The permitted column names.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_set",
        kwargs={"column_set": column_set, "exact_match": True},
        meta=None,
    )
