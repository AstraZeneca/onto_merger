"""Prepare data sets: example, integration test"""
import os
from pathlib import Path
from typing import List

import pandas as pd
from pandas import DataFrame

from onto_merger.analyser import analysis_util
from onto_merger.data.constants import (
    COLUMN_DEFAULT_ID,
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    DIRECTORY_OUTPUT,
    RDFS_SUBCLASS_OF,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MAPPINGS,
    TABLE_MERGES_AGGREGATED,
    TABLE_NODES,
    TABLE_NODES_CONNECTED_ONLY,
    TABLE_NODES_MERGED,
    TABLE_NODES_OBSOLETE,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import DataRepository, NamedTable


def prune_nodes_by_namespace(nodes_raw: DataFrame,
                             node_namespaces_to_remove: List[str]) -> DataFrame:
    default_id_ns_column_name = analysis_util.get_namespace_column_name_for_column(COLUMN_DEFAULT_ID)
    nodes_updated = nodes_raw.query(
        expr=f"{default_id_ns_column_name} != @node_nss",
        local_dict={"node_nss": node_namespaces_to_remove},
        inplace=False
    )
    node_namespaces_raw = sorted(list(set(nodes_raw[default_id_ns_column_name].tolist())))
    node_namespaces_pruned = sorted(list(set(nodes_updated[default_id_ns_column_name].tolist())))
    print(f"Node count\n\traw = {len(nodes_raw):,d}\n\tpruned = {len(nodes_updated):,d}")
    print(f"Node namespaces\n\traw = {len(node_namespaces_raw)} | {node_namespaces_raw}"
          + f"\n\tpruned = {len(node_namespaces_pruned)} | {node_namespaces_pruned}")
    return nodes_updated


def prune_edges_by_namespace(edges_raw: DataFrame,
                             node_namespaces_to_remove: List[str]) -> DataFrame:
    src_ns_column_name = analysis_util.get_namespace_column_name_for_column(COLUMN_SOURCE_ID)
    trg_ns_column_name = analysis_util.get_namespace_column_name_for_column(COLUMN_TARGET_ID)
    edges_pruned = edges_raw.query(
        expr=f"{src_ns_column_name} != @node_nss & {trg_ns_column_name} != @node_nss",
        local_dict={"node_nss": node_namespaces_to_remove},
        inplace=False
    )
    namespaces_raw = sorted(
        list(set((edges_raw[src_ns_column_name].tolist()) + edges_raw[trg_ns_column_name].tolist())))
    namespaces_pruned = sorted(
        list(set((edges_pruned[src_ns_column_name].tolist()) + edges_pruned[trg_ns_column_name].tolist())))
    print(f"Edge count\n\traw = {len(edges_raw):,d}\n\tpruned = {len(edges_pruned):,d}")
    print(f"Edge namespaces\n\traw = {len(namespaces_raw)} | {namespaces_raw}"
          + f"\n\tpruned = {len(namespaces_pruned)} | {namespaces_pruned}")
    return edges_pruned


def prune_mappings_by_type(edges_raw: DataFrame,
                           edge_types_to_remove: List[str]) -> DataFrame:
    edges_pruned = edges_raw.query(
        expr=f"{COLUMN_RELATION} != @rela_types",
        local_dict={"rela_types": edge_types_to_remove},
        inplace=False
    )
    edge_types_raw = sorted(list(set(edges_raw[COLUMN_RELATION].tolist())))
    edge_types_pruned = sorted(list(set(edges_pruned[COLUMN_RELATION].tolist())))
    print(f"Mapping count\n\traw = {len(edges_raw):,d}\n\tpruned = {len(edges_pruned):,d}")
    print(f"Mapping types\n\traw = {len(edge_types_raw)} | {edge_types_raw}"
          + f"\n\tpruned = {len(edge_types_pruned)} | {edge_types_pruned}")
    return edges_pruned


def produce_example_data_set(raw_input_path: str = "bikg_2022-02-28-4.27.0_disease") -> None:
    """Produces the data set used to run a full example."""

    # (1) raw input load data
    data_manager_raw_input = DataManager(project_folder_path=raw_input_path, clear_output_directory=False)
    raw_input_data: List[NamedTable] = analysis_util.add_namespace_column_to_loaded_tables(
        data_manager_raw_input.load_input_tables()
    )
    data_repo: DataRepository = DataRepository()
    data_repo.update(tables=raw_input_data)
    print(data_repo.get_repo_summary())

    # (2) prune by namespace
    node_nss_to_remove = ["OMIM", "UMLS", "NCI", "CHEBI", "BOWES", "NCIT", "UK_BIOBANK", "BOWES_2012", "MP"]

    # (2/a) prune nodes to map
    print("\nPrune TABLE_NODES by namespace")
    nodes_to_map_updated = NamedTable(
        TABLE_NODES,
        prune_nodes_by_namespace(
            nodes_raw=data_repo.get(TABLE_NODES).dataframe,
            node_namespaces_to_remove=node_nss_to_remove
        )
    )

    # (2/b) prune obsolete nodes
    print("\nPrune TABLE_NODES_OBSOLETE by namespace")
    nodes_obsolete_updated = NamedTable(
        TABLE_NODES_OBSOLETE,
        prune_nodes_by_namespace(
            nodes_raw=data_repo.get(TABLE_NODES_OBSOLETE).dataframe,
            node_namespaces_to_remove=node_nss_to_remove
        )
    )

    # (2/c) prune mappings
    print("\nPrune TABLE_MAPPINGS by namespace")
    mappings_ns_pruned = prune_edges_by_namespace(
        edges_raw=data_repo.get(TABLE_MAPPINGS).dataframe,
        node_namespaces_to_remove=node_nss_to_remove
    )
    print("\nPrune TABLE_MAPPINGS by mapping type")
    mappings_updated = NamedTable(
        TABLE_MAPPINGS,
        prune_mappings_by_type(
            edges_raw=mappings_ns_pruned,
            edge_types_to_remove=["alexion_orphanet_omim_exact"]
        )
    )

    # (2/d) prune hierarchy edges
    print("\nPrune TABLE_EDGES_HIERARCHY by namespace")
    hierarchy_edges_updated = NamedTable(
        TABLE_EDGES_HIERARCHY,
        prune_edges_by_namespace(
            edges_raw=data_repo.get(TABLE_EDGES_HIERARCHY).dataframe,
            node_namespaces_to_remove=node_nss_to_remove
        )
    )

    # (3) save EXAMPLE data
    pruned_example_project_path = os.path.abspath("bikg_disease/input")
    Path(pruned_example_project_path).mkdir(parents=True, exist_ok=True)
    for table in [nodes_to_map_updated, nodes_obsolete_updated, mappings_updated, hierarchy_edges_updated]:
        file_path = os.path.join(pruned_example_project_path, f"{table.name}.csv")
        table.dataframe.to_csv(file_path, index=False)


def produce_test_data_set(raw_path: str = "bikg_2022-02-28-4.27.0_disease",
                          example_data_set_path: str = "bikg_disease") -> None:
    # load example
    data_manager_example_input = DataManager(project_folder_path=example_data_set_path, clear_output_directory=False)
    raw_input_data: List[NamedTable] = analysis_util.add_namespace_column_to_loaded_tables(
        data_manager_example_input.load_input_tables()
    )
    data_repo: DataRepository = DataRepository()
    data_repo.update(tables=raw_input_data)
    print(data_repo.get_repo_summary())

    # load process results from raw
    nodes_merged = pd.read_csv(os.path.abspath(
        f"{raw_path}/{DIRECTORY_OUTPUT}/{TABLE_NODES_MERGED}.csv"))[COLUMN_DEFAULT_ID].tolist()
    nodes_connected = pd.read_csv(os.path.abspath(
        f"{raw_path}/{DIRECTORY_OUTPUT}/{TABLE_NODES_CONNECTED_ONLY}.csv"))[COLUMN_DEFAULT_ID].tolist()
    nodes_to_keep = list(set(nodes_merged + nodes_connected))
    print(f"TABLE_NODES_MERGED = {len(nodes_merged):,d}")
    print(f"TABLE_NODES_CONNECTED_ONLY {len(nodes_connected):,d}")
    print(f"nodes_to_keep {len(nodes_to_keep):,d}")

    # prune nodes to map: keep merged, some connected, but reduce overall size significantly
    nodes_updated_to_keep = data_repo.get(TABLE_NODES).dataframe.query(
        expr=f"{COLUMN_DEFAULT_ID} == @node_ids ",
        local_dict={"node_ids": nodes_to_keep},
        inplace=False
    )[[COLUMN_DEFAULT_ID]]
    nodes_some_to_keep = data_repo.get(TABLE_NODES).dataframe.query(
        expr=f"{COLUMN_DEFAULT_ID} != @node_ids ",
        local_dict={"node_ids": nodes_to_keep},
        inplace=False
    )
    default_id_ns_column_name = analysis_util.get_namespace_column_name_for_column(COLUMN_DEFAULT_ID)
    namespaces = list(set(nodes_some_to_keep[default_id_ns_column_name].tolist()))
    print(f"nodes_updated_to_keep = {len(nodes_updated_to_keep):,d}")
    print(f"nodes_some_to_keep = {len(nodes_some_to_keep):,d} | namespaces = {namespaces}")
    nodes_some_to_keep_dfs = []
    for ns in namespaces:
        nodes_for_ns = data_repo.get(TABLE_NODES).dataframe.query(
            expr=f"{default_id_ns_column_name} == '{ns}' ",
            inplace=False
        )[[COLUMN_DEFAULT_ID]].head(1000)
        nodes_some_to_keep_dfs.append(nodes_for_ns)
    nodes_some_to_keep_reduced = pd.concat(nodes_some_to_keep_dfs)
    print(f"nodes_some_to_keep_reduced = {len(nodes_some_to_keep_reduced):,d}")
    nodes_to_keep_final = pd.concat([nodes_some_to_keep_reduced, nodes_updated_to_keep])
    print(f"nodes_to_keep_final = {len(nodes_to_keep_final):,d}")

    # prune hierarchy edges such that it loads fast but still supports the connectivity process
    hierarchy_post = pd.read_csv(os.path.abspath(f"{raw_path}/{DIRECTORY_OUTPUT}/{TABLE_EDGES_HIERARCHY_POST}.csv")) \
        [[COLUMN_SOURCE_ID, COLUMN_TARGET_ID, COLUMN_RELATION, COLUMN_PROVENANCE]]
    hierarchy_post[COLUMN_RELATION] = RDFS_SUBCLASS_OF
    hierarchy_post[COLUMN_PROVENANCE] = "TEST_DATA"

    merges = pd.read_csv(os.path.abspath(
        f"{raw_path}/{DIRECTORY_OUTPUT}/{TABLE_MERGES_AGGREGATED}.csv"))
    merge_map = {}
    for _, row in merges.iterrows():
        if row[COLUMN_TARGET_ID] not in merge_map:
            merge_map[row[COLUMN_TARGET_ID]] = []
        if not row[COLUMN_SOURCE_ID].startswith("MONDO"):
            merge_map[row[COLUMN_TARGET_ID]].append(row[COLUMN_SOURCE_ID])

    def get_target_id(current_trg_id: str, source_id: str):
        source_id_ns = source_id.split(":")[0]
        if current_trg_id in merge_map:
            target_ids = merge_map[current_trg_id]
            for trg_id in target_ids:
                if trg_id.startswith(source_id_ns):
                    return trg_id
            # get one for ns
        return current_trg_id

    hierarchy_post[COLUMN_TARGET_ID] = hierarchy_post.apply(
        lambda x: get_target_id(current_trg_id=x[COLUMN_TARGET_ID], source_id=x[COLUMN_SOURCE_ID]),
        axis=1
    )
    print(f"hierarchy_post = {len(hierarchy_post):,d}")
    print(f"merges = {len(merges):,d}")

    # save
    test_data_project_path = os.path.abspath("test_data/input")
    Path(test_data_project_path).mkdir(parents=True, exist_ok=True)
    nodes_to_keep_final.to_csv(os.path.join(test_data_project_path, "nodes.csv"), index=False)
    hierarchy_post[[COLUMN_SOURCE_ID, COLUMN_TARGET_ID, COLUMN_RELATION, COLUMN_PROVENANCE]] \
        .to_csv(os.path.join(test_data_project_path, "edges_hierarchy.csv"), index=False)


produce_test_data_set()
