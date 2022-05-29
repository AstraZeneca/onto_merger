"""GE Expectation configuration helper methods."""

from typing import List, Union

from great_expectations.core import ExpectationConfiguration

from onto_merger.analyser.analysis_utils import get_namespace_column_name_for_column
from onto_merger.data.constants import (
    COLUMN_COUNT_UNMAPPED_NODES,
    COLUMN_DEFAULT_ID,
    COLUMN_MAPPING_TYPE_GROUP,
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE,
    COLUMN_SOURCE_ID,
    COLUMN_SOURCE_TO_TARGET,
    COLUMN_TARGET_ID,
    DOMAIN_SUFFIX,
    ONTO_MERGER,
    RELATION_MERGE,
    RELATION_RDFS_SUBCLASS_OF,
    SCHEMA_ALIGNMENT_STEPS_TABLE,
    SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_CONNECTIVITY_STEPS_REPORT,
    TABLE_EDGES_HIERARCHY_DOMAIN,
    TABLE_MAPPINGS_DOMAIN,
    TABLE_MERGES_DOMAIN,
    TABLE_NAME_TO_TABLE_SCHEMA_MAP,
    TABLES_EDGE,
    TABLES_EDGE_HIERARCHY,
    TABLES_MAPPING,
    TABLES_MERGE_INTERMEDIATE,
    TABLES_NODE,
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
    if table_name in TABLES_NODE:
        return produce_node_table_expectations(table_name=table_name)
    elif table_name in TABLES_EDGE:
        return produce_edge_table_expectations(table_name=table_name, alignment_config=alignment_config)
    elif table_name == TABLE_ALIGNMENT_STEPS_REPORT:
        return produce_report_alignment_steps_expectations(alignment_config=alignment_config)
    elif table_name == TABLE_CONNECTIVITY_STEPS_REPORT:
        return produce_report_connectivity_steps_expectations(alignment_config=alignment_config)
    else:
        return []


def produce_node_table_expectations(table_name: str) -> List[ExpectationConfiguration]:
    """Produce the  list of relevant ExpectationConfiguration-s for a given node table.

    :return: The list of relevant ExpectationConfiguration-s.
    """
    column_set = list(TABLE_NAME_TO_TABLE_SCHEMA_MAP[table_name])
    if not is_domain_table(table_name=table_name):
        column_set.append(get_namespace_column_name_for_column(COLUMN_DEFAULT_ID))
    expectations = [produce_expectation_config_expect_table_columns_to_match_set(column_set=column_set)]
    expectations.extend(produce_node_short_id_expectations(column_name=COLUMN_DEFAULT_ID, is_node_table=True))
    if not is_domain_table(table_name=table_name):
        expectations.extend(
            produce_node_namespace_expectations(
                column_name=get_namespace_column_name_for_column(node_id_column=COLUMN_DEFAULT_ID)
            )
        )
    return expectations


def produce_edge_table_expectations(
    table_name: str, alignment_config: AlignmentConfig
) -> List[ExpectationConfiguration]:
    """Produce the list of ExpectationConfiguration-s for an edge table (mapping, merge, hierarchy).

    :param table_name: The edge table type (hierarchy, mapping, merge).
    :param alignment_config: The alignment process configuration dataclass.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    # columns
    expectations = [
        produce_expectation_config_expect_table_columns_to_match_set(
            column_set=get_column_set_for_edge_table(table_name=table_name)
        )
    ]
    # node IDs and namespaces, prov
    for node_column in [COLUMN_SOURCE_ID, COLUMN_TARGET_ID]:
        if not is_domain_table(table_name=table_name):
            expectations.extend(produce_node_short_id_expectations(column_name=node_column, is_node_table=False))
            expectations.extend(
                produce_node_namespace_expectations(
                    column_name=get_namespace_column_name_for_column(node_id_column=node_column)
                )
            )
            expectations.extend(
                produce_source_to_target_node_namespace_expectations(column_name=COLUMN_SOURCE_TO_TARGET)
            )
    if is_domain_table(table_name=table_name):
        expectations.extend(produce_node_short_id_expectations(column_name=COLUMN_TARGET_ID, is_node_table=False))
        if table_name == TABLE_MERGES_DOMAIN:
            expectations.extend(produce_node_short_id_expectations(column_name=COLUMN_SOURCE_ID, is_node_table=True))
            expectations.extend(produce_prov_expectations(column_name=COLUMN_PROVENANCE, value_set=[ONTO_MERGER]))
        if table_name in [TABLE_MAPPINGS_DOMAIN, TABLE_EDGES_HIERARCHY_DOMAIN]:
            expectations.extend(produce_node_short_id_expectations(column_name=COLUMN_SOURCE_ID, is_node_table=False))
            expectations.extend(produce_prov_expectations(column_name=COLUMN_PROVENANCE, value_set=[]))
    # relation
    if table_name not in TABLES_MERGE_INTERMEDIATE:
        expectations.extend(
            produce_edge_relation_expectations(
                column_name=COLUMN_RELATION,
                edge_types=get_relation_types_for_edge_table(table_name=table_name, alignment_config=alignment_config),
            )
        )
    return expectations


def get_column_set_for_edge_table(table_name: str) -> List[str]:
    """Return the column names (table schema) for a given edge table.

    :param table_name: The edge table type (hierarchy, mapping, merge).
    :return: The list of columns for the given table.
    """
    if table_name not in TABLE_NAME_TO_TABLE_SCHEMA_MAP:
        return []
    column_set = TABLE_NAME_TO_TABLE_SCHEMA_MAP[table_name]
    if not is_domain_table(table_name=table_name):
        column_set.extend(
            [
                get_namespace_column_name_for_column(COLUMN_SOURCE_ID),
                get_namespace_column_name_for_column(COLUMN_TARGET_ID),
                COLUMN_SOURCE_TO_TARGET,
            ]
        )
    return column_set


def get_relation_types_for_edge_table(table_name: str, alignment_config: AlignmentConfig) -> List[str]:
    """Produce a list of edge types that are permitted in the given edge table.

    Edge hierarchy table can only have 'rdfs:subClassOf' relation, whereas a mapping table
    can have different edge, i.e. mapping relation, types as specified in the
    AlignmentConfig.

    :param table_name: The edge table type.
    :param alignment_config: The alignment process configuration dataclass.
    :return: The list of permitted edge types for the given table.
    """
    if table_name in TABLES_EDGE_HIERARCHY:
        return [RELATION_RDFS_SUBCLASS_OF]
    elif table_name == TABLE_MERGES_DOMAIN:
        return [RELATION_MERGE]
    elif table_name in TABLES_MAPPING:
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


def produce_node_namespace_expectations(column_name: str) -> List[ExpectationConfiguration]:
    """Produce the list of ExpectationConfiguration-s for a node namespace column.

    Node namespaces are found in all tables (used for processing and analysis).

    :param column_name: The column that contains the node namespace.
    otherwise repeated IDs are permitted in the column.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    return [
        produce_expectation_config_expect_column_to_exist(column_name=column_name),
        produce_expectation_config_column_type_string(column_name=column_name),
        produce_expectation_config_column_values_to_not_be_null(column_name=column_name),
        produce_expectation_config_column_value_lengths_to_be_between(
            column_name=column_name, min_value=2, max_value=15
        ),
        produce_expectation_config_column_values_to_match_regex(column_name=column_name, regex="^[A-Za-z][\\w]*$"),
    ]


def produce_source_to_target_node_namespace_expectations(column_name: str) -> List[ExpectationConfiguration]:
    """Produce the list of ExpectationConfiguration-s for a node namespace column.

    :param column_name: The column that contains the node namespace.
    otherwise repeated IDs are permitted in the column.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    return [
        produce_expectation_config_expect_column_to_exist(column_name=column_name),
        produce_expectation_config_column_type_string(column_name=column_name),
        produce_expectation_config_column_values_to_not_be_null(column_name=column_name),
        produce_expectation_config_column_value_lengths_to_be_between(
            column_name=column_name, min_value=6, max_value=30
        ),
        produce_expectation_config_column_values_to_match_regex(
            column_name=column_name, regex="^[A-Za-z][\\w]* to [A-Za-z][\\w]*$"
        ),
    ]


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


def produce_prov_expectations(column_name: str, value_set: List[str]) -> List[ExpectationConfiguration]:
    """Produce the ExpectationConfiguration-s for the provenance column.

    :param column_name: The column that contains the provenance.
    :param value_set: The expected prov values.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    expectations = [
        produce_expectation_config_expect_column_to_exist(column_name=column_name),
        produce_expectation_config_column_type_string(column_name=column_name),
        produce_expectation_config_column_values_to_not_be_null(column_name=column_name),
        produce_expectation_config_column_value_lengths_to_be_between(
            column_name=column_name, min_value=3, max_value=15
        ),
    ]
    if value_set:
        expectations.append(
            produce_expectation_config_expect_column_values_to_be_in_set(column_name=column_name, value_set=value_set),
        )
    return expectations


def produce_report_alignment_steps_expectations(alignment_config: AlignmentConfig) -> List[ExpectationConfiguration]:
    """Produce the ExpectationConfiguration-s for the report table.

    :param alignment_config: The alignment process configuration dataclass.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    # table shape and edge case column expectations
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={"column_list": SCHEMA_ALIGNMENT_STEPS_TABLE},
            meta=None,
        ),
        produce_expectation_config_expect_column_values_to_be_in_set(
            column_name=COLUMN_MAPPING_TYPE_GROUP,
            value_set=alignment_config.mapping_type_groups.get_mapping_type_group_names(),
        ),
        # produce_expectation_config_expect_column_values_to_be_in_set(
        #    column_name=COLUMN_SOURCE,
        #    value_set=["START"]
        # )
    ] + produce_count_column_expectations(
        column_name=COLUMN_COUNT_UNMAPPED_NODES,
        min_value=0,
        max_value=None,
        decreasing=True,
    )

    # add count columns with general settings
    columns = list(
        set(list(SCHEMA_ALIGNMENT_STEPS_TABLE))
        - {COLUMN_MAPPING_TYPE_GROUP, COLUMN_COUNT_UNMAPPED_NODES, COLUMN_SOURCE}
    )
    for column_name in columns:
        expectations.extend(
            produce_count_column_expectations(
                column_name=column_name,
                min_value=0,
                max_value=None,
                decreasing=False,
            )
        )

    return expectations


def produce_report_connectivity_steps_expectations(alignment_config: AlignmentConfig) -> List[ExpectationConfiguration]:
    """Produce the ExpectationConfiguration-s for the report table.

    :param alignment_config: The alignment process configuration dataclass.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    # table shape and edge case column expectations
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={"column_list": SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE},
            meta=None,
        ),
        # produce_expectation_config_expect_column_values_to_be_in_set(
        #    column_name=COLUMN_SOURCE,
        #    value_set=[]
        # )
    ]

    # add count columns with general settings
    columns = list(set(list(SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE)) - {COLUMN_SOURCE})
    for column_name in columns:
        expectations.extend(
            produce_count_column_expectations(
                column_name=column_name,
                min_value=0,
                max_value=None,
                decreasing=False,
            )
        )

    return expectations


def produce_count_column_expectations(
    column_name: str, min_value: int, max_value: Union[None, int], decreasing: bool
) -> List[ExpectationConfiguration]:
    """Produce the ExpectationConfiguration-s for a column containing counts.

    :param column_name: The column with count values.
    :param min_value: The count minimum value.
    :param max_value: The count maximum value, if None it is not checked.
    :param decreasing: If true the counts should be decreasing; otherwise it is False.
    :return: The list of relevant ExpectationConfiguration-s.
    """
    expectations = [
        produce_expectation_config_expect_column_to_exist(column_name=column_name),
        produce_expectation_config_column_values_to_not_be_null(column_name=column_name),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": column_name, "type_": "int64", "mostly": 1.00},
            meta=None,
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": column_name, "min_value": min_value, "max_value": max_value, "mostly": 1.00},
            meta=None,
        ),
    ]
    if decreasing is True:
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_decreasing",
                kwargs={"column": column_name, "mostly": 1.00},
                meta=None,
            )
        )

    return expectations


def produce_expectation_config_column_type_string(
    column_name: str,
) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration for a given string column to check that it only has string type objects.

    :param column_name: The name of the column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": column_name, "type_": "object", "mostly": 1.00},
        meta=None,
    )


def produce_expectation_config_column_values_to_not_be_null(
    column_name: str,
) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration for a given column to check whether it contains any null values.

    :param column_name: The name of the column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": column_name, "mostly": 1.00},
        meta=None,
    )


def produce_expectation_config_column_values_to_be_unique(
    column_name: str,
) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration to check whether a given column has only unique values.

    :param column_name: The name of the column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": column_name, "mostly": 1.00},
        meta=None,
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


def produce_expectation_config_expect_column_to_exist(
    column_name: str,
) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration to check the existence a given column in a table.

    :param column_name: The name of the table column to be tested with this expectation.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": column_name},
        meta=None,
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


def produce_expectation_config_expect_table_columns_to_match_set(
    column_set: List[str],
) -> ExpectationConfiguration:
    """Produce an ExpectationConfiguration to check whether a given table has only the specified columns.

    :param column_set: The permitted column names.
    :return: The ExpectationConfiguration object.
    """
    return ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_set",
        kwargs={"column_set": column_set, "exact_match": True},
        meta=None,
    )


def is_domain_table(table_name: str) -> bool:
    """Check if the table is one of the final domain ontology parts.

    :param table_name: The name of the table.
    :return: True if it is a domain table, otherwise False.
    """
    return DOMAIN_SUFFIX in table_name
