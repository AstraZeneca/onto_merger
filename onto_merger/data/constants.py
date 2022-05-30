"""Data file constants."""

# FILE NAMES
from typing import List

FILE_NAME_CONFIG_JSON = "config.json"
FILE_NAME_LOG = "onto-merger.logger"

# PROCESS DIRECTORIES
DIRECTORY_INPUT = "input"
DIRECTORY_OUTPUT = "output"
DIRECTORY_INTERMEDIATE = "intermediate"
DIRECTORY_DOMAIN_ONTOLOGY = "domain_ontology"
DIRECTORY_REPORT = "report"
DIRECTORY_DROPPED_MAPPINGS = "dropped_mappings"
DIRECTORY_PROFILED_DATA = "data_profile_reports"
DIRECTORY_DATA_TESTS = "data_tests"
DIRECTORY_LOGS = "logs"
DIRECTORY_ANALYSIS = "analysis"

# COLUMNS
COLUMN_DEFAULT_ID = "default_id"
COLUMN_SOURCE_ID = "source_id"
COLUMN_TARGET_ID = "target_id"
COLUMN_RELATION = "relation"
COLUMN_PROVENANCE = "prov"
COLUMN_SOURCE_TO_TARGET = "source_to_target"
COLUMN_NAMESPACE = "namespace"
COLUMN_STEP_COUNTER = "step_counter"
COLUMN_SOURCE_ID_ALIGNED_TO = "aligned_to_source"
COLUMN_MAPPING_TYPE_GROUP = "mapping_type_group"
COLUMN_MAPPING_HASH = "comparison_hash"
COLUMN_COUNT = "count"
COLUMN_FREQUENCY = "frequency"
COLUMN_COUNT_UNMAPPED_NODES = "count_unmapped_nodes"
COLUMN_SOURCE = "source"
NODE_ID_COLUMNS = [COLUMN_DEFAULT_ID, COLUMN_SOURCE_ID, COLUMN_TARGET_ID]

# COLUMN VALUES
RELATION_RDFS_SUBCLASS_OF = "rdfs:subClassOf"
RELATION_MERGE = "merge"
ONTO_MERGER = "ONTO_MERGER"

# ENTITY TYPES
TABLE_TYPE_NODE = "node"
TABLE_TYPE_EDGE = "edge"
TABLE_TYPE_MAPPING = "mapping"

# INPUT TABLES
TABLE_NODES = "nodes"
TABLE_NODES_OBSOLETE = "nodes_obsolete"
TABLE_MAPPINGS = "mappings"
TABLE_EDGES_HIERARCHY = "edges_hierarchy"

# INTERMEDIATE TABLES
TABLE_NODES_SEED = "nodes_seed"
TABLE_NODES_MERGED = "nodes_merged"
TABLE_NODES_MERGED_TO_SEED = "nodes_merged_to_seed"
TABLE_NODES_UNMAPPED = "nodes_unmapped"
TABLE_NODES_CONNECTED = "nodes_connected"
TABLE_NODES_DANGLING = "nodes_dangling"
TABLE_EDGES_HIERARCHY_POST = "edges_hierarchy_post"
TABLE_MERGES = "merges"
TABLE_MERGES_WITH_META_DATA = "merges_with_meta_data"
TABLE_MERGES_AGGREGATED = "merges_aggregated"
TABLE_MAPPINGS_UPDATED = "mappings_updated"
TABLE_MAPPINGS_FOR_INPUT_NODES = "mappings_for_input_nodes"
TABLE_MAPPINGS_OBSOLETE_TO_CURRENT = "mappings_obsolete_to_current"
TABLE_ALIGNMENT_STEPS_REPORT = "alignment_steps_report"
TABLE_CONNECTIVITY_STEPS_REPORT = "connectivity_steps_report"
TABLE_PIPELINE_STEPS_REPORT = "pipeline_steps_report"

# DOMAIN ONTOLOGY TABLES
DOMAIN_SUFFIX = "_domain"
TABLE_NODES_DOMAIN = TABLE_NODES + DOMAIN_SUFFIX
TABLE_MAPPINGS_DOMAIN = TABLE_MAPPINGS + DOMAIN_SUFFIX
TABLE_MERGES_DOMAIN = TABLE_MERGES + DOMAIN_SUFFIX
TABLE_EDGES_HIERARCHY_DOMAIN = TABLE_EDGES_HIERARCHY + DOMAIN_SUFFIX

# TABLE GROUPS
TABLES_INPUT: List[str] = [
    TABLE_NODES,
    TABLE_NODES_OBSOLETE,
    TABLE_MAPPINGS,
    TABLE_EDGES_HIERARCHY,
]
TABLES_INTERMEDIATE: List[str] = [
    TABLE_NODES_SEED,
    TABLE_NODES_UNMAPPED,
    TABLE_NODES_MERGED,
    TABLE_NODES_MERGED_TO_SEED,
    TABLE_NODES_CONNECTED,
    TABLE_NODES_DANGLING,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MERGES_WITH_META_DATA,
    TABLE_MERGES_AGGREGATED,
    TABLE_MAPPINGS_UPDATED,
    TABLE_MAPPINGS_FOR_INPUT_NODES,
    TABLE_MAPPINGS_OBSOLETE_TO_CURRENT,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_CONNECTIVITY_STEPS_REPORT,
    TABLE_PIPELINE_STEPS_REPORT,
]
TABLES_DOMAIN: List[str] = [
    TABLE_NODES_DOMAIN,
    TABLE_MAPPINGS_DOMAIN,
    TABLE_MERGES_DOMAIN,
    TABLE_EDGES_HIERARCHY_DOMAIN,
]
TABLES_OUTPUT: List[str] = [
    TABLE_NODES,
    TABLE_MAPPINGS,
    TABLE_MERGES,
    TABLE_EDGES_HIERARCHY,
]
TABLES_REPORT: List[str] = [
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_CONNECTIVITY_STEPS_REPORT,
]
TABLES_NODE: List[str] = [
    TABLE_NODES,
    TABLE_NODES_CONNECTED,
    TABLE_NODES_DANGLING,
    TABLE_NODES_DOMAIN,
    TABLE_NODES_MERGED,
    TABLE_NODES_OBSOLETE,
    TABLE_NODES_UNMAPPED,
]
TABLES_EDGE_HIERARCHY = [
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_DOMAIN,
    TABLE_EDGES_HIERARCHY_POST,
]
TABLES_MAPPING : List[str]= [
    TABLE_MAPPINGS,
    TABLE_MAPPINGS_DOMAIN,
    TABLE_MAPPINGS_UPDATED,
    TABLE_MAPPINGS_OBSOLETE_TO_CURRENT,
]
TABLES_MERGE: List[str] = [
    TABLE_MERGES,
    TABLE_MERGES_AGGREGATED,
    TABLE_MERGES_DOMAIN,
    TABLE_MERGES_WITH_META_DATA,
]
TABLES_MERGE_INTERMEDIATE: List[str] = [
    TABLE_MERGES,
    TABLE_MERGES_AGGREGATED,
    TABLE_MERGES_WITH_META_DATA,
]
TABLES_EDGE: List[str] = TABLES_EDGE_HIERARCHY + TABLES_MERGE + TABLES_MAPPING

# TABLE SCHEMAS
SCHEMA_NODE_ID_LIST_TABLE: List[str] = [COLUMN_DEFAULT_ID]
SCHEMA_EDGE_SOURCE_TO_TARGET_IDS: List[str] = [COLUMN_SOURCE_ID, COLUMN_TARGET_ID]
SCHEMA_MERGE_TABLE: List[str] = list(SCHEMA_EDGE_SOURCE_TO_TARGET_IDS)
SCHEMA_MERGE_TABLE_WITH_META_DATA: List[str] = [
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    COLUMN_STEP_COUNTER,
    COLUMN_SOURCE_ID_ALIGNED_TO,
    COLUMN_MAPPING_TYPE_GROUP,
]
SCHEMA_HIERARCHY_EDGE_TABLE: List[str] = [
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    COLUMN_RELATION,
    COLUMN_PROVENANCE,
]
SCHEMA_MAPPING_TABLE: List[str] = list(SCHEMA_HIERARCHY_EDGE_TABLE)
SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE: List[str] = [
    COLUMN_NAMESPACE,
    COLUMN_COUNT,
    COLUMN_FREQUENCY,
]
SCHEMA_DATA_REPO_SUMMARY: List[str] = ["Table", "Count", "Columns"]
SCHEMA_ALIGNMENT_STEPS_TABLE: List[str] = [
    COLUMN_MAPPING_TYPE_GROUP,
    COLUMN_SOURCE,
    COLUMN_STEP_COUNTER,
    COLUMN_COUNT_UNMAPPED_NODES,
    "count_mappings",
    "count_nodes_one_source_to_many_target",
    "count_merged_nodes",
    "task",
    "start",
    "start_date_time",
    "end",
    "elapsed",
]
SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE: List[str] = [
    COLUMN_STEP_COUNTER,
    COLUMN_SOURCE,
    COLUMN_COUNT_UNMAPPED_NODES,
    "count_reachable_unmapped_nodes",
    "count_available_edges",
    "count_produced_edges",
    "count_connected_nodes",
    "task",
    "start",
    "start_date_time",
    "end",
    "elapsed",
]
SCHEMA_PIPELINE_STEPS_REPORT_TABLE: List[str] = [
    "task",
    "start",
    "end",
    "elapsed",
]
TABLE_NAME_TO_TABLE_SCHEMA_MAP = {
    TABLE_NODES: list(SCHEMA_NODE_ID_LIST_TABLE),
    TABLE_NODES_OBSOLETE: list(SCHEMA_NODE_ID_LIST_TABLE),
    TABLE_NODES_MERGED: list(SCHEMA_NODE_ID_LIST_TABLE),
    TABLE_NODES_UNMAPPED: list(SCHEMA_NODE_ID_LIST_TABLE),
    TABLE_NODES_CONNECTED: list(SCHEMA_NODE_ID_LIST_TABLE),
    TABLE_NODES_DANGLING: list(SCHEMA_NODE_ID_LIST_TABLE),
    TABLE_NODES_DOMAIN: list(SCHEMA_NODE_ID_LIST_TABLE),
    TABLE_EDGES_HIERARCHY: list(SCHEMA_HIERARCHY_EDGE_TABLE),
    TABLE_EDGES_HIERARCHY_DOMAIN: list(SCHEMA_HIERARCHY_EDGE_TABLE),
    TABLE_EDGES_HIERARCHY_POST: list(SCHEMA_HIERARCHY_EDGE_TABLE),
    TABLE_MAPPINGS: list(SCHEMA_MAPPING_TABLE),
    TABLE_MAPPINGS_DOMAIN: list(SCHEMA_MAPPING_TABLE),
    TABLE_MAPPINGS_UPDATED: list(SCHEMA_MAPPING_TABLE),
    TABLE_MAPPINGS_OBSOLETE_TO_CURRENT: list(SCHEMA_MAPPING_TABLE),
    TABLE_MAPPINGS_FOR_INPUT_NODES: list(SCHEMA_MAPPING_TABLE),
    TABLE_MERGES: list(SCHEMA_MERGE_TABLE),
    TABLE_MERGES_DOMAIN: list(SCHEMA_MERGE_TABLE) + [COLUMN_RELATION, COLUMN_PROVENANCE],
    TABLE_MERGES_WITH_META_DATA: list(SCHEMA_MERGE_TABLE_WITH_META_DATA),
    TABLE_MERGES_AGGREGATED: list(SCHEMA_MERGE_TABLE),
}

# MAPPING_TYPE_GROUPS
MAPPING_TYPE_GROUP_EQV = "equivalence"
MAPPING_TYPE_GROUP_XREF = "database_reference"
