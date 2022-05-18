"""Table descriptions explaining columns of tables presented in the report."""

from onto_merger.report.data.constants import (
    SECTION_INPUT,
    SECTION_OUTPUT,
    SECTION_OVERVIEW,
    SECTION_ALIGNMENT,
    SECTION_CONNECTIVITY,
    SECTION_DATA_PROFILING,
    SECTION_DATA_TESTS,
)

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

# SUMMARY SUBSECTION DESCRIPTIONS #
section_summary_description_overview = """..."""
section_summary_description_input = """..."""
section_summary_description_output = """..."""
section_summary_description_alignment = """..."""
section_summary_description_connectivity = """..."""
section_summary_description_data_profiling = """..."""
section_summary_description_data_test = """..."""


# LOADERS #
def load_section_summary_description_data(section_name: str) -> str:
    if section_name == SECTION_OVERVIEW:
        return section_summary_description_overview
    if section_name == SECTION_INPUT:
        return section_summary_description_input
    if section_name == SECTION_OUTPUT:
        return section_summary_description_output
    if section_name == SECTION_ALIGNMENT:
        return section_summary_description_alignment
    if section_name == SECTION_CONNECTIVITY:
        return section_summary_description_connectivity
    if section_name == SECTION_DATA_PROFILING:
        return section_summary_description_data_profiling
    if section_name == SECTION_DATA_TESTS:
        return section_summary_description_data_test
    return "..."


def load_table_description_data(table_name: str) -> str:
    # todo
    # if table_name == SECTION_OVERVIEW:
    #     return table_section_summary_description_overview
    # if table_name == SECTION_INPUT:
    #     return table_section_summary_description_input
    # if table_name == SECTION_OUTPUT:
    #     return table_section_summary_description_output
    # if table_name == SECTION_ALIGNMENT:
    #     return table_section_summary_description_alignment
    # if table_name == SECTION_CONNECTIVITY:
    #     return table_section_summary_description_connectivity
    # if table_name == SECTION_DATA_PROFILING:
    #     return table_section_summary_description_data_profiling
    # if table_name == SECTION_DATA_TESTS:
    #     return table_section_summary_description_data_test
    return "..."
