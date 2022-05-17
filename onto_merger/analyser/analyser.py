import pandas as pd
from pandas import DataFrame

from onto_merger.analyser.analysis_util import produce_table_with_namespace_column_for_node_ids, \
    produce_table_node_namespace_distribution
from onto_merger.alignment.hierarchy_utils import produce_node_id_table_from_edge_table
from onto_merger.data.constants import SCHEMA_NODE_ID_LIST_TABLE, COLUMN_DEFAULT_ID, COLUMN_COUNT, \
    SCHEMA_HIERARCHY_EDGE_TABLE, COLUMN_PROVENANCE, COLUMN_RELATION, COLUMN_SOURCE_ID

TEST_PATH = "/Users/kmnb265/Desktop/test_data"
COVERED = "covered"


def load_csv(table_path: str) -> DataFrame:
    df = pd.read_csv(table_path).drop_duplicates(keep="first", ignore_index=True)
    print(f"Loaded table [{table_path.split('/')[-1]}] with {len(df):,d} row(s).")
    return df


def print_df_stats(df, name):
    print("\n\n", name, "\n\t", len(df), "\n\t", list(df))
    print(df.head(10))


def produce_node_namespace_distribution_with_type(nodes: DataFrame, metric_name: str) -> DataFrame:
    node_namespace_distribution_df = produce_table_node_namespace_distribution(
        node_table=produce_table_with_namespace_column_for_node_ids(table=nodes)
    )[["namespace", "count"]]
    df = node_namespace_distribution_df.rename(
        columns={"count": f"{metric_name}_count"}
    )
    return df


def produce_node_covered_by_edge_table(nodes: DataFrame,
                                       edges: DataFrame,
                                       coverage_column: str) -> DataFrame:
    node_id_list_of_edges = produce_node_id_table_from_edge_table(edges=edges)
    node_id_list_of_edges[coverage_column] = True
    nodes_covered = pd.merge(
        nodes[SCHEMA_NODE_ID_LIST_TABLE],
        node_id_list_of_edges,
        how="left",
        on=COLUMN_DEFAULT_ID,
    ).dropna(subset=[coverage_column])
    print(f"covered nodes = {len(nodes_covered):,d} | nodes = {len(nodes):,d} | edges = {len(edges):,d}")
    return nodes_covered[SCHEMA_NODE_ID_LIST_TABLE]


def produce_node_analysis(nodes: DataFrame, mappings: DataFrame, edges_hierarchy: DataFrame) -> DataFrame:
    node_namespace_distribution_df = produce_node_namespace_distribution_with_type(
        nodes=nodes, metric_name="namespace"
    )

    node_mapping_coverage_df = produce_node_covered_by_edge_table(nodes=nodes,
                                                                  edges=mappings,
                                                                  coverage_column=COVERED)
    node_mapping_coverage_distribution_df = produce_node_namespace_distribution_with_type(
        nodes=node_mapping_coverage_df, metric_name="mapping_coverage"
    )

    node_edge_coverage_df = produce_node_covered_by_edge_table(nodes=nodes,
                                                               edges=edges_hierarchy,
                                                               coverage_column=COVERED)
    node_edge_coverage_distribution_df = produce_node_namespace_distribution_with_type(
        nodes=node_edge_coverage_df, metric_name="edge_coverage"
    )

    # merge
    node_analysis = node_namespace_distribution_df
    if len(node_edge_coverage_distribution_df) > 0:
        node_analysis = pd.merge(
            node_analysis,
            node_mapping_coverage_distribution_df,
            how="outer",
            on="namespace",
        )
    else:
        node_analysis["mapping_coverage_count"] = 0
    if len(node_edge_coverage_distribution_df) > 0:
        node_analysis = pd.merge(
            node_analysis,
            node_edge_coverage_distribution_df,
            how="outer",
            on="namespace",
        ).fillna(0, inplace=False)
    else:
        node_analysis["edge_coverage_count"] = 0

    # add freq
    node_analysis["namespace_freq"] = node_analysis.apply(
        lambda x: ((x['namespace_count'] / len(nodes)) * 100), axis=1
    )
    # add relative freq
    node_analysis["mapping_coverage_freq"] = node_analysis.apply(
        lambda x: (x['mapping_coverage_count'] / x['namespace_count'] * 100), axis=1
    )
    node_analysis["edge_coverage_freq"] = node_analysis.apply(
        lambda x: (x['edge_coverage_count'] / x['namespace_count'] * 100), axis=1
    )

    print_df_stats(node_analysis, "node_analysis")

    return node_analysis


def produce_mapping_analysis_for_type(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_RELATION, COLUMN_SOURCE_ID]].groupby([COLUMN_RELATION])\
        .count()\
        .reset_index()\
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_COUNT})\
        .sort_values(COLUMN_COUNT, ascending=False)
    print_df_stats(df, "type")
    return df

def produce_mapping_analysis_for_type_and_prov(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_RELATION, COLUMN_PROVENANCE])\
        .count()\
        .reset_index()\
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_COUNT})\
        .sort_values(COLUMN_COUNT, ascending=False)
    print_df_stats(df, "type and prov")
    return df

def produce_mapping_analysis_for_prov_and_type(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_RELATION, COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_PROVENANCE, COLUMN_RELATION])\
        .count()\
        .reset_index()\
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_COUNT})\
        .sort_values(COLUMN_COUNT, ascending=False)
    print_df_stats(df, "prob and type")
    return df


def produce_mapping_analysis_for_prov(mappings: DataFrame) -> DataFrame:
    df = mappings[[COLUMN_PROVENANCE, COLUMN_SOURCE_ID]].groupby([COLUMN_PROVENANCE])\
        .count()\
        .reset_index()\
        .rename(columns={COLUMN_SOURCE_ID: COLUMN_COUNT})\
        .sort_values(COLUMN_COUNT, ascending=False)
    print_df_stats(df, "prov")
    return df


def produce_mapping_analysis_for_mapped_nss(mappings: DataFrame) -> DataFrame:
    pass


def produce_mapping_analysis(mappings: DataFrame) -> DataFrame:

    # mappings_with_ns = produce_table_with_namespace_column_for_node_ids(mappings)

    analysis_for_prov = produce_mapping_analysis_for_prov(mappings=mappings)
    analysis_for_type = produce_mapping_analysis_for_type(mappings=mappings)
    # analysis_for_type_prov = produce_mapping_analysis_for_type_and_prov(mappings=mappings)
    # analysis_for_prov_type = produce_mapping_analysis_for_prov_and_type(mappings=mappings)


def analysis_input_dataset():
    # input_nodes = load_csv(f"{TEST_PATH}/input/nodes.csv")
    # input_nodes_obs = load_csv(f"{TEST_PATH}/input/nodes_obsolete.csv")[SCHEMA_NODE_ID_LIST_TABLE]
    # input_mappings = load_csv(f"{TEST_PATH}/input/mappings.csv")
    # input_edges_hierarchy = load_csv(f"{TEST_PATH}/input/edges_hierarchy.csv")
    # print_df_stats(input_nodes_obs, "input_nodes_obs")

    # # nodes
    # analysis_input_node = produce_node_analysis(nodes=input_nodes,
    #                                             mappings=input_mappings,
    #                                             edges_hierarchy=input_edges_hierarchy)
    # analysis_input_node.to_csv(f"{TEST_PATH}/output/intermediate/analysis/input_nodes.csv")
    #
    # # nodes_obs
    # analysis_input_node_obs = produce_node_analysis(nodes=input_nodes_obs,
    #                                                 mappings=input_mappings,
    #                                                 edges_hierarchy=input_edges_hierarchy)
    # analysis_input_node_obs.to_csv(f"{TEST_PATH}/output/intermediate/analysis/input_nodes_obsolete.csv")

    # mappings: per types, per prov, per mapped, for chart
    produce_mapping_analysis(mappings=load_csv(f"{TEST_PATH}/input/mappings 2.csv"))

    # edges


analysis_input_dataset()
