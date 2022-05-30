import pandas as pd
from tqdm import tqdm

PATH_MAIN = "/Users/kmnb265/Desktop/ONTOMERGE_Data/bikg_2022-02-28-4.27.0_disease_full_background_knowledge"
PATH_INTER = f"{PATH_MAIN}/output/intermediate"

# # INPUT
# input_df = pd.read_csv(f"{PATH_MAIN}/input/nodes.csv").drop_duplicates(keep="first")
# input_nodes = input_df["default_id"].tolist()
# print(f"input_df = {len(input_df):,d}")
#
#
# def compare_to_input(produced_nodes):
#     nodes_not_in_input = [n for n in tqdm(produced_nodes) if n not in input_nodes]
#     print(f"nodes_not_in_input = {len(nodes_not_in_input)}\n")
#     if len(nodes_not_in_input):
#         print(f"{nodes_not_in_input[0:20]}\n\n")
#
# # PRODUCED
# nodes_unmapped_df = pd.read_csv(f"{PATH_INTER}/nodes_unmapped.csv").drop_duplicates(keep="first")
# print(f"\nnodes_unmapped_df = {len(nodes_unmapped_df):,d}")
# compare_to_input(nodes_unmapped_df["default_id"].tolist())
#
# nodes_seed_df = pd.read_csv(f"{PATH_INTER}/nodes_seed.csv").drop_duplicates(keep="first")
# print(f"\nnodes_seed_df = {len(nodes_seed_df):,d}")
# compare_to_input(nodes_seed_df["default_id"].tolist())
#
# nodes_merged_df = pd.read_csv(f"{PATH_INTER}/nodes_merged.csv").drop_duplicates(keep="first")
# print(f"\nnodes_merged_df = {len(nodes_merged_df):,d}")
# compare_to_input(nodes_merged_df["default_id"].tolist())
#
# nodes_merged_to_seed_df = pd.read_csv(f"{PATH_INTER}/nodes_merged_to_seed.csv").drop_duplicates(keep="first")
# print(f"\nnodes_merged_to_seed_df = {len(nodes_merged_to_seed_df):,d}")
# compare_to_input(nodes_merged_to_seed_df["default_id"].tolist())


def merge_mappings():
    COLUMN_SOURCE_ID = "source_id"
    COLUMN_TARGET_ID = "target_id"
    COLUMN_RELATION = "relation"
    COLUMN_PROVENANCE = "prov"
    SCHEMA_HIERARCHY_EDGE_TABLE = [
        COLUMN_SOURCE_ID,
        COLUMN_TARGET_ID,
        COLUMN_RELATION,
        COLUMN_PROVENANCE,
    ]
    mps_background = pd.read_csv(f"{PATH_MAIN}/input/mappings_from_background.csv")[SCHEMA_HIERARCHY_EDGE_TABLE]
    mps_bikg = pd.read_csv(f"{PATH_MAIN}/input/mappings_from_bikg.csv")[SCHEMA_HIERARCHY_EDGE_TABLE]
    df = pd.concat([mps_background, mps_bikg]).drop_duplicates(keep="first")
    df.to_csv(f"{PATH_MAIN}/input/mappings.csv", index=False)

merge_mappings()

