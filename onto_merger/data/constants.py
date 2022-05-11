"""Data file constants."""

# CONFIG
CONFIG_JSON = "config.json"

# INPUT TABLES
TABLE_NODES = "nodes"
TABLE_NODES_OBSOLETE = "nodes_obsolete"
TABLE_MAPPINGS = "mappings"
TABLE_EDGES_HIERARCHY = "edges_hierarchy"
INPUT_TABLES = [
    TABLE_NODES,
    TABLE_NODES_OBSOLETE,
    TABLE_MAPPINGS,
    TABLE_EDGES_HIERARCHY,
]

# OUTPUT TABLES
TABLE_NODES_MERGED = "nodes_merged"
TABLE_NODES_UNMAPPED = "nodes_unmapped"
TABLE_NODES_CONNECTED_ONLY = "nodes_only_connected"
TABLE_NODES_DANGLING = "nodes_dangling"
TABLE_EDGES_HIERARCHY_POST = "edges_hierarchy_post"
TABLE_MERGES = "merges"
TABLE_MERGES_AGGREGATED = "merges_aggregated"
TABLE_MAPPINGS_UPDATED = "mappings_updated"
TABLE_MAPPINGS_OBSOLETE_TO_CURRENT = "mappings_obsolete_to_current"
TABLE_ALIGNMENT_STEPS_REPORT = "alignment_steps_report"
TABLE_CONNECTIVITY_STEPS_REPORT = "connectivity_steps_report"
OUTPUT_TABLES = [
    TABLE_NODES_UNMAPPED,
    TABLE_NODES_MERGED,
    TABLE_NODES_CONNECTED_ONLY,
    TABLE_NODES_DANGLING,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MERGES,
    TABLE_MERGES_AGGREGATED,
    TABLE_MAPPINGS_UPDATED,
    TABLE_MAPPINGS_OBSOLETE_TO_CURRENT,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_CONNECTIVITY_STEPS_REPORT
]

# EDGE TABLE TYPES
EDGE = "edge"
MAPPING = "mapping"
MERGE = "merge"

# HIERARCHY RELATION
RDFS_SUBCLASS_OF = "rdfs:subClassOf"

# COLUMNS
COLUMN_DEFAULT_ID = "default_id"
COLUMN_SOURCE_ID = "source_id"
COLUMN_TARGET_ID = "target_id"
NODE_ID_COLUMNS = [COLUMN_DEFAULT_ID, COLUMN_SOURCE_ID, COLUMN_TARGET_ID]

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

# TABLE SCHEMAS
SCHEMA_EDGE_SOURCE_TO_TARGET_IDS = [COLUMN_SOURCE_ID, COLUMN_TARGET_ID]
SCHEMA_MERGE_TABLE = SCHEMA_EDGE_SOURCE_TO_TARGET_IDS
SCHEMA_MERGE_TABLE_WITH_META_DATA = [
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    COLUMN_STEP_COUNTER,
    COLUMN_SOURCE_ID_ALIGNED_TO,
    COLUMN_MAPPING_TYPE_GROUP,
]
SCHEMA_HIERARCHY_EDGE_TABLE = [
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    COLUMN_RELATION,
    COLUMN_PROVENANCE,
]
SCHEMA_MAPPING_TABLE = [
    COLUMN_SOURCE_ID,
    COLUMN_TARGET_ID,
    COLUMN_RELATION,
    COLUMN_PROVENANCE,
]
SCHEMA_NODE_NAMESPACE_FREQUENCY_TABLE = [
    COLUMN_NAMESPACE,
    COLUMN_COUNT,
    COLUMN_FREQUENCY,
]
SCHEMA_DATA_REPO_SUMMARY = ["Table", "Count", "Columns"]
SCHEMA_ALIGNMENT_STEPS_TABLE = [
    COLUMN_MAPPING_TYPE_GROUP,
    "source",
    COLUMN_STEP_COUNTER,
    "count_unmapped_nodes",
    "count_mappings",
    "count_nodes_one_source_to_many_target",
    "count_merged_nodes",
]
SCHEMA_TABLE_CONNECTIVITY_STEPS_REPORT = [
    "source_id",
    "count_unmapped_node_ids",
    "count_reachable_unmapped_nodes",
    "count_available_edges",
    "count_produced_edges",
    "count_connected_nodes"
]

# MAPPING_TYPE_GROUPS
MAPPING_TYPE_GROUP_EQV = "equivalence"
MAPPING_TYPE_GROUP_XREF = "database_reference"

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
FILE_NAME_LOG = "onto-merger.logger"
