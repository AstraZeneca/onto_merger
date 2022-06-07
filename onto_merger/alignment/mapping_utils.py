"""Helper methods to work with mappings."""

from typing import List

import pandas as pd
from pandas import DataFrame

from onto_merger.analyser.analysis_utils import (
    get_namespace_column_name_for_column,
    produce_table_node_ids_from_edge_table,
    produce_table_with_namespace_column_for_node_ids,
)
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_MAPPING_HASH,
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    SCHEMA_MAPPING_TABLE,
    TABLE_MERGES_WITH_META_DATA,
)
from onto_merger.data.dataclasses import NamedTable
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def get_mappings_internal_node_reassignment(mappings: DataFrame) -> DataFrame:
    """Filter a mapping set so each remaining mapping is between nodes of the same ontology.

    :param mappings: The input mapping set ot be filtered.
    :return: The internal code re-assigment mappings table.
    """
    query = (
        f"{get_namespace_column_name_for_column(COLUMN_SOURCE_ID)} == "
        + f"{get_namespace_column_name_for_column(COLUMN_TARGET_ID)}"
    )
    mapping_subset = produce_table_with_namespace_column_for_node_ids(table=mappings).query(expr=query, inplace=False)[
        SCHEMA_MAPPING_TABLE
    ]
    logger.info(
        f"Found {len(mapping_subset)} 'internal_node_reassignment' mappings from total " + f"{len(mappings)} mappings."
    )
    return mapping_subset


def filter_out_mappings_internal_node_reassignment(mappings: DataFrame) -> DataFrame:
    """Filter a mapping set so each remaining mapping is between nodes of the same ontology.

    :param mappings: The input mapping set ot be filtered.
    :return: The internal code re-assigment mappings table.
    """
    query = (
        f"{get_namespace_column_name_for_column(COLUMN_SOURCE_ID)} != "
        + f"{get_namespace_column_name_for_column(COLUMN_TARGET_ID)}"
    )
    mapping_subset = produce_table_with_namespace_column_for_node_ids(table=mappings).query(expr=query, inplace=False)[
        SCHEMA_MAPPING_TABLE
    ]
    logger.info(
        f"Filtered out {len(mappings) - len(mapping_subset)} mappings from total " + f"{len(mappings)} mappings."
    )
    return mapping_subset


def get_mappings_obsolete_to_current_node_id(nodes_obsolete: DataFrame, mappings: DataFrame) -> DataFrame:
    """Return the internal node ID re-assingment mappings.

    Given a set of internal_node_reassignment mappings, ensures that the obsolete
    (deprecated) node IDs are always in the source_id column, and the current node IDs
    are in the target_id column.

    :param nodes_obsolete: The table of obsolete node IDs.
    :param mappings: The input that contains internal_node_reassignment mappings.
    :return:
    """
    nodes_obsolete_ids = list(nodes_obsolete[COLUMN_DEFAULT_ID])
    df = get_mappings_internal_node_reassignment(mappings=mappings)

    df["has_obs_node"] = df.apply(
        lambda x: True
        if ((x[COLUMN_SOURCE_ID] in nodes_obsolete_ids) | (x[COLUMN_TARGET_ID] in nodes_obsolete_ids))
        else False,
        axis=1,
    )
    df.query("has_obs_node == True", inplace=True)

    updated_src_id_column = f"updated_{COLUMN_SOURCE_ID}"
    df[updated_src_id_column] = df.apply(
        lambda x: x[COLUMN_SOURCE_ID] if x[COLUMN_SOURCE_ID] in nodes_obsolete_ids else x[COLUMN_TARGET_ID],
        axis=1,
    )

    updated_trg_id_column = f"updated_{COLUMN_TARGET_ID}"
    df[updated_trg_id_column] = df.apply(
        lambda x: x[COLUMN_SOURCE_ID] if x[COLUMN_TARGET_ID] in nodes_obsolete_ids else x[COLUMN_TARGET_ID],
        axis=1,
    )

    df.drop(columns=[COLUMN_SOURCE_ID, COLUMN_TARGET_ID], inplace=True)
    df.rename(
        columns={updated_src_id_column: COLUMN_SOURCE_ID, updated_trg_id_column: COLUMN_TARGET_ID},
        inplace=True,
    )

    df = df[SCHEMA_MAPPING_TABLE]

    return df


def get_mappings_with_updated_node_ids(
    mappings: DataFrame, mappings_obsolete_to_current_node_id: DataFrame
) -> DataFrame:
    """Update the obsolete node IDs in a mapping set.

    :param mappings: The set of mappings to be updated.
    :param mappings_obsolete_to_current_node_id: The obsolete to current node ID
    mappings.
    :return: The updated mapping set.
    """
    # internal mappings that would apply
    node_ids_in_mappings = produce_table_node_ids_from_edge_table(edges=mappings)[COLUMN_DEFAULT_ID].tolist()
    mappings_obsolete_to_current_node_id_applicable = mappings_obsolete_to_current_node_id.query(
        expr=(f"{COLUMN_SOURCE_ID} == @node_ids_in_mappings"),
        local_dict={"node_ids_in_mappings": node_ids_in_mappings},
        inplace=False,
    )
    logger.info(f"Out of {len(mappings_obsolete_to_current_node_id)} obsolete_to_current_node_id mappings, "
                + f"{len(mappings_obsolete_to_current_node_id_applicable)} can be applied to mappings.")

    mappings_updated = filter_out_mappings_internal_node_reassignment(mappings=mappings)
    mappings_updated = update_mappings_with_current_node_ids(
        mappings_internal_obsolete_to_current_node_id=mappings_obsolete_to_current_node_id_applicable,
        mappings=mappings_updated,
    )

    return mappings_updated


def get_nodes_with_updated_node_ids(
    nodes: DataFrame, mappings_obsolete_to_current_node_id: DataFrame
) -> DataFrame:
    """Produce the node table with only current node IDs.

    :param nodes: The original (input) node table
    :param mappings_obsolete_to_current_node_id: The node ID update mappings.
    :return: The updated node table.
    """
    # internal mappings that would apply
    mappings_obsolete_to_current_node_id_applicable = mappings_obsolete_to_current_node_id.copy().query(
        expr=(f"{COLUMN_SOURCE_ID} == @node_ids_in_mappings"),
        local_dict={"node_ids_in_mappings": nodes[COLUMN_DEFAULT_ID].tolist()},
        inplace=False,
    )
    logger.info(f"Out of {len(mappings_obsolete_to_current_node_id)} obsolete_to_current_node_id mappings, "
                + f"{len(mappings_obsolete_to_current_node_id_applicable)} can be applied to nodes.")

    return mappings_obsolete_to_current_node_id_applicable


def add_comparison_column_for_reoriented_mappings(mappings: DataFrame):
    """Produce a column from the source and target ID as an ordered list to compare mappings with different orientation.

    :param mappings: The input mapping set.
    :return: The mapping set appended with the comparison column.
    """
    mappings[COLUMN_MAPPING_HASH] = mappings.apply(
        lambda x: (
            f"{sorted([x[COLUMN_SOURCE_ID], x[COLUMN_TARGET_ID]])}" + f"|{x[COLUMN_RELATION]}|{x[COLUMN_PROVENANCE]}"
        ),
        axis=1,
    )
    return mappings


def get_mappings_for_namespace(namespace: str, edges: DataFrame) -> DataFrame:
    """Filter a mapping set for a given namespace.

    The result will only contain mapping where either the source or the target node
    is from the specified ontology (namespace).

    :param namespace: The ontology namespace.
    :param edges: The input mapping set.
    :return: The filtered mapping set.
    """
    mappings_with_ns = produce_table_with_namespace_column_for_node_ids(table=edges)
    query = (
        f"({get_namespace_column_name_for_column(COLUMN_SOURCE_ID)} == '{namespace}') "
        + f"| ({get_namespace_column_name_for_column(COLUMN_TARGET_ID)} == "
        + f"'{namespace}')"
    )
    mapping_subset = mappings_with_ns.query(expr=query, inplace=False)[SCHEMA_MAPPING_TABLE]
    logger.info(
        f"Found {len(mapping_subset):,d} edges (mapping or hierarchy edges) for namespace '{namespace}'"
        + f" from total {len(edges):,d}."
    )
    return mapping_subset


def get_source_to_target_mappings_for_multiplicity(mappings: DataFrame, is_one_or_many_to_one: bool) -> DataFrame:
    """Filter a mapping set according to multiplicity.

    Return either the mapping subset that align one or many source node to exactly
    one target node (stable set used for merges), or the subset that align one source
    node to many target nodes (unstable set that is dropped).

    The mapping subsets are not pruned i.e. the filtering is based on multiplicity
    analysis of the source nodes.

    :param mappings: The input mapping set.
    :param is_one_or_many_to_one: If True it return the stable mappings, if False it
    returns the unstable mappings.
    :return: The filtered mapping set, either stable or unstable.
    """
    # group from source perspective
    df = (
        mappings[[COLUMN_SOURCE_ID, COLUMN_TARGET_ID]]
        .groupby(COLUMN_SOURCE_ID)
        .agg({COLUMN_TARGET_ID: lambda x: set(x)})
        .reset_index()
    )
    df["target_size"] = df[COLUMN_TARGET_ID].apply(lambda x: len(x))
    df_one_to_one = df[df["target_size"] == 1]
    df_one_to_many = df[df["target_size"] > 1]

    # drop non one to one mappings
    if is_one_or_many_to_one:
        source_ids_to_drop = list(df_one_to_many[COLUMN_SOURCE_ID])
    else:
        source_ids_to_drop = list(df_one_to_one[COLUMN_SOURCE_ID])
    mapping_subset = mappings.query(
        f"{COLUMN_SOURCE_ID} != @node_ids",
        local_dict={"node_ids": source_ids_to_drop},
        inplace=False,
    )
    return mapping_subset


def get_one_or_many_source_to_one_target_mappings(mappings: DataFrame) -> DataFrame:
    """Return only one or many source to one target mappings.

    Filter a mapping set to return the mapping subset that align one or
    many source node to exactly one target node (stable set used for merges).

    :param mappings: The input mapping set to be filtered.
    :return: The filtered (stable) mapping set.
    """
    mapping_subset = get_source_to_target_mappings_for_multiplicity(mappings=mappings, is_one_or_many_to_one=True)
    logger.info(
        f"Found {len(mapping_subset)} one_or_many_source_to_one_target mappings from " + f"{len(mappings)} mappings."
    )
    return mapping_subset


def get_one_source_to_many_target_mappings(mappings: DataFrame) -> DataFrame:
    """Return only one source to many target mappings.

    Filter a mapping set to return the mapping subset that align one source
     node to many target nodes (unstable set that is dropped)

    :param mappings: The input mapping set to be filtered.
    :return: The filtered (unstable) mapping set.
    """
    mapping_subset = get_source_to_target_mappings_for_multiplicity(mappings=mappings, is_one_or_many_to_one=False)
    logger.info(
        f"Found {len(mapping_subset)} one_or_many_source_to_one_target mappings from " + f"{len(mappings)} mappings."
    )
    return mapping_subset.sort_values([COLUMN_SOURCE_ID, COLUMN_TARGET_ID])


def update_mappings_with_current_node_ids(
    mappings_internal_obsolete_to_current_node_id: DataFrame,
    mappings: DataFrame,
) -> DataFrame:
    """Update a mapping set with current node IDs.

    :param mappings_internal_obsolete_to_current_node_id: The obsolete-to-current node ID mappings.
    :param mappings: The input mapping set to be updated.
    :return: The updated mapping set.
    """
    # src
    df = pd.merge(
        mappings[SCHEMA_MAPPING_TABLE],
        mappings_internal_obsolete_to_current_node_id[[COLUMN_SOURCE_ID, COLUMN_TARGET_ID]].rename(
            columns={COLUMN_TARGET_ID: "new_src"},
            inplace=False,
        ),
        how="left",
        on=COLUMN_SOURCE_ID,
    )

    # trg
    df = pd.merge(
        df,
        mappings_internal_obsolete_to_current_node_id[[COLUMN_SOURCE_ID, COLUMN_TARGET_ID]].rename(
            columns={COLUMN_TARGET_ID: "new_trg", COLUMN_SOURCE_ID: COLUMN_TARGET_ID},
            inplace=False,
        ),
        how="left",
        on=COLUMN_TARGET_ID,
    )
    # clean df
    df["src"] = df["new_src"].mask(pd.isnull, df[COLUMN_SOURCE_ID])
    df["trg"] = df["new_trg"].mask(pd.isnull, df[COLUMN_TARGET_ID])
    df.drop([COLUMN_SOURCE_ID, COLUMN_TARGET_ID], axis=1, inplace=True)
    df.rename(
        columns={"src": COLUMN_SOURCE_ID, "trg": COLUMN_TARGET_ID},
        inplace=True,
    )
    return df[SCHEMA_MAPPING_TABLE]


def orient_mappings_to_namespace(required_target_id_namespace: str, mappings: DataFrame) -> DataFrame:
    """Update a mapping so the target node is always of the specified ontology (namespace).

    The mapping set assumed to contain only mappings where either the source or the
    target node is from the specified namespace.

    :param required_target_id_namespace: The ontology namespace for the target node ID.
    :param mappings: The input mapping set to be updated.
    :return: The updated mapping set.
    """
    df = produce_table_with_namespace_column_for_node_ids(table=mappings)
    if len(df) == 0:
        return mappings
    updated_src_id_column = f"updated_{COLUMN_SOURCE_ID}"
    updated_trg_id_column = f"updated_{COLUMN_TARGET_ID}"
    df[updated_src_id_column] = df.apply(
        lambda x: x[COLUMN_SOURCE_ID]
        if x[get_namespace_column_name_for_column(COLUMN_SOURCE_ID)] != required_target_id_namespace
        else x[COLUMN_TARGET_ID],
        axis=1,
    )
    df[updated_trg_id_column] = df.apply(
        lambda x: x[COLUMN_TARGET_ID]
        if x[get_namespace_column_name_for_column(COLUMN_TARGET_ID)] == required_target_id_namespace
        else x[COLUMN_SOURCE_ID],
        axis=1,
    )
    df.drop([COLUMN_SOURCE_ID, COLUMN_TARGET_ID], axis=1, inplace=True)
    df.rename(
        columns={updated_src_id_column: COLUMN_SOURCE_ID, updated_trg_id_column: COLUMN_TARGET_ID},
        inplace=True,
    )
    return df[SCHEMA_MAPPING_TABLE]


def get_mappings_with_mapping_relations(permitted_mapping_relations: List[str], mappings: DataFrame) -> DataFrame:
    """Filter a mapping set for permitted mapping relations.

    :param permitted_mapping_relations: The list of permitted mapping relations.
    :param mappings: The input mapping set to be filtered.
    :return: The filtered mapping set.
    """
    query = f"{COLUMN_RELATION} == @permitted_mapping_relations"
    mapping_subset = mappings.query(expr=query, inplace=False)
    logger.info(
        f"Found {len(mapping_subset):,d} for relation(s) "
        + f"'{permitted_mapping_relations}' from {len(mappings):,d} mappings."
    )
    return mapping_subset


def deduplicate_mappings_for_type_group(mapping_type_group_name: str, mappings: DataFrame) -> DataFrame:
    """Produce a set of unique mappings by removing duplicates.

    Mappings between the same source and target node with mapping relations that are in the same type group.
    We assume that all input mappings are from the same type group.

    :param mapping_type_group_name: The name of the mapping type group (e.g.
    equivalence) that will be used as the mapping relation for the deduplicated
    mappings.
    :param mappings: The input mapping set to be deduplicated.
    :return: The deduplicated mapping set.
    """
    mappings_deduplicated = mappings.copy()
    mappings_deduplicated[COLUMN_RELATION] = mapping_type_group_name
    mappings_deduplicated.drop_duplicates(keep="first", inplace=True)
    logger.info(
        f"Deduplicated '{mapping_type_group_name}' mappings, "
        + f"input size: {len(mappings):,d} output size: "
        + f"{len(mappings_deduplicated):,d}."
    )
    return mappings_deduplicated


def filter_mappings_for_input_node_set(input_nodes: DataFrame, mappings: DataFrame) -> DataFrame:
    """Filter a mapping set so it only contains mappings referencing nodes from the input set.

    :param input_nodes: The set of input nodes.
    :param mappings: The mapping set to be filtered.
    :return: The filtered mapping set.
    """
    node_ids_to_keep = list(input_nodes[COLUMN_DEFAULT_ID])
    mapping_subset = mappings.query(
        f"({COLUMN_SOURCE_ID} == @node_ids) and ({COLUMN_TARGET_ID} == @node_ids)",
        local_dict={"node_ids": node_ids_to_keep},
        inplace=False,
    )
    logger.info(
        f"Found {len(mapping_subset):,d} mappings (from total {len(mappings):,d}) "
        + f"for {len(input_nodes):,d} input_nodes."
    )
    return mapping_subset


def filter_mappings_for_node_set(nodes: DataFrame, mappings: DataFrame) -> DataFrame:
    """Filter a mapping set such that the source node IDs must belong to the specified node ID list.

    :param nodes: The dataframe containing the permitted source node IDs.
    :param mappings: The input mapping set to be filtered.
    :return: The filtered mapping set.
    """
    node_ids_to_keep = list(nodes[COLUMN_DEFAULT_ID])
    mapping_subset = mappings.query(
        f"{COLUMN_SOURCE_ID} == @node_ids",
        local_dict={"node_ids": node_ids_to_keep},
        inplace=False,
    )
    logger.info(
        f"Found {len(mapping_subset):,d} mappings (from total {len(mappings):,d}) " + f"for {len(nodes):,d} nodes."
    )
    return mapping_subset


def produce_self_merges_for_seed_nodes(seed_id: str, nodes: DataFrame, nodes_obsolete: DataFrame) -> NamedTable:
    """Produce a set of (self) merges for the seed ontology.

    In the result each source and target node ID are the same.
    This is used for counting mapped nodes.

    :param seed_id: The seed ontology name.
    :param nodes: The set of all nodes.
    :param nodes_obsolete: The set of obsolete nodes.
    :return: The merged table.
    """
    # get only seed nodes
    df = produce_table_with_namespace_column_for_node_ids(table=nodes)
    df.query(
        f"{get_namespace_column_name_for_column(COLUMN_DEFAULT_ID)} == '{seed_id}'",
        inplace=True,
    )
    df.rename(
        columns={COLUMN_DEFAULT_ID: COLUMN_SOURCE_ID},
        inplace=True,
    )

    # filter out obsolete nodes
    nodes_obsolete_ids = list(nodes_obsolete[COLUMN_DEFAULT_ID])
    df.query(
        f"{COLUMN_SOURCE_ID} != @node_ids",
        local_dict={"node_ids": nodes_obsolete_ids},
        inplace=True,
    )
    df = df[[COLUMN_SOURCE_ID]]

    # produce self merges
    df[COLUMN_TARGET_ID] = df[COLUMN_SOURCE_ID].apply(lambda x: x)

    logger.info(f"Produced {len(df):,d} self merges for seed source '{seed_id}'.")
    return NamedTable(name=TABLE_MERGES_WITH_META_DATA, dataframe=df)
