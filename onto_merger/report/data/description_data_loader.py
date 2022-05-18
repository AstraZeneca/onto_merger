"""Table descriptions explaining columns of tables presented in the report."""



import pandas as pd
from pandas import DataFrame

# COLUMN DESCRIPTIONS #
table_description_node_summary = [
    {"column": "Node Origin",
     "description": "The ontology (or namespace) the node originates from."},
    {"column": "Input",
     "description": "Nodes that assume to belong to the same domain that most likely "
                    + "contain duplicated, connected and overlapping nodes."},
    {"column": "Merged",
     "description": "Nodes that are mapped and merged onto other nodes."},
    {"column": "Only connected",
     "description": "Nodes that are not merged onto other nodes, but are connected to the hierarchy."},
    {"column": "Dangling",
     "description": "Nodes that are not neither merged nor connected."},
]
table_description_node_analysis = [
    {"column": "Origin",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
    {"column": "Mapping coverage",
     "description": "..."},
    {"column": "Hierarchy coverage",
     "description": "..."},
]
table_description_mapping_analysis = [
    {"column": "Mapped nodes",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
    {"column": "Types",
     "description": "..."},
    {"column": "Provenances",
     "description": "..."},
]
table_description_merge_analysis = [
    {"column": "Canonical",
     "description": "..."},
    {"column": "Merged",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
]
table_description_edge_hierarchy_analysis = [
    {"column": "Sub to Super",
     "description": "..."},
    {"column": "Count",
     "description": "..."},
    {"column": "Frequency",
     "description": "..."},
    {"column": "Provenance",
     "description": "..."},
]
table_description_alignment_steps = [
    {"column": "Step",
     "description": ""},
    {"column": "Mapping type",
     "description": ""},
    {"column": "Source",
     "description": ""},
    {"column": "Unmapped",
     "description": ""},
    {"column": "Merged",
     "description": ""},
    {"column": "Mappings",
     "description": ""},
    {"column": "Dropped",
     "description": ""},
]
table_description_connectivity_steps = [
    {"column": "Step",
     "description": ""},
    {"column": "Source",
     "description": ""},
    {"column": "Unmapped",
     "description": ""},
    {"column": "Reachable",
     "description": ""},
    {"column": "Connected",
     "description": ""},
    {"column": "Edges (available)",
     "description": ""},
    {"column": "Edges (produced)",
     "description": ""},
]

p = "/Users/kmnb265/Documents/GitHub/onto_merger/onto_merger/report/data/table_column_descriptions"

pd.DataFrame(table_description_node_summary).to_csv(f"{p}/node_summary.csv", index=False)
pd.DataFrame(table_description_node_analysis).to_csv(f"{p}/node_analysis.csv", index=False)
pd.DataFrame(table_description_mapping_analysis).to_csv(f"{p}/mapping_analysis.csv", index=False)
pd.DataFrame(table_description_merge_analysis).to_csv(f"{p}/merge_analysis.csv", index=False)
pd.DataFrame(table_description_edge_hierarchy_analysis).to_csv(f"{p}/edge_hierarchy_analysis.csv", index=False)
pd.DataFrame(table_description_alignment_steps).to_csv(f"{p}/alignment_steps.csv", index=False)
pd.DataFrame(table_description_connectivity_steps).to_csv(f"{p}/connectivity_steps.csv", index=False)
