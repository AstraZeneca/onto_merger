"""Data classes and helper methods."""

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from dataclasses_json import dataclass_json
from pandas import DataFrame

from onto_merger.data.constants import (
    TABLES_INPUT,
    TABLES_INTERMEDIATE,
    SCHEMA_DATA_REPO_SUMMARY,
    TABLES_DOMAIN)


@dataclass_json
@dataclass
class AlignmentConfigMappingTypeGroups:
    """Mapping type groups configuration."""

    equivalence: List[str]
    database_reference: List[str]
    label_match: List[str]

    @property
    def all_mapping_types(self) -> List[str]:
        """Produce a single list containing all mapping relations.

        :return: All mappings relations.
        """
        return self.equivalence + self.database_reference + self.label_match

    def get_mapping_type_group_names(self) -> List[str]:
        return [str(k) for k in self.__dict__.keys()]

@dataclass_json
@dataclass
class AlignmentConfigBase:
    """Base alignment process configuration."""

    domain_node_type: str
    seed_ontology_name: str


@dataclass
class AlignmentConfig:
    """Alignment process configuration."""

    base_config: AlignmentConfigBase
    mapping_type_groups: AlignmentConfigMappingTypeGroups
    as_dict: dict


@dataclass
class NamedTable:
    """Wrap a Pandas dataframe with its name (identifier) for convenient access and serialisation."""

    name: str
    dataframe: DataFrame


class DataRepository:
    """Store named tables in a dictionary and provides access and update convenience methods."""

    def __init__(self):
        """Initialise the DataRepository dataclass."""
        self.data: Dict[str, NamedTable] = {}

    def get(self, table_name: str) -> NamedTable:
        """Return a named table for a given table identifier.

        :param table_name: The table identifier.
        :return: The named table.
        """
        table = self.data.get(table_name)
        if table is None:
            raise Exception
        else:
            return table

    def get_input_tables(self) -> List[NamedTable]:
        """Return the list of input named tables.

        :return: The list of input named tables.
        """
        return [self.get(table_name=table_name) for table_name in TABLES_INPUT if table_name in self.data]

    def get_intermediate_tables(self) -> List[NamedTable]:
        """Return the list of intermediate named tables.

        :return: The list of intermediate named tables.
        """
        return [self.get(table_name=table_name) for table_name in TABLES_INTERMEDIATE if table_name in self.data]

    def get_domain_tables(self) -> List[NamedTable]:
        """Return the list of domain named tables.

        :return: The list of domain named tables.
        """
        return [self.get(table_name=table_name) for table_name in TABLES_DOMAIN if table_name in self.data]

    def update(
        self,
        table: Optional[NamedTable] = None,
        tables: Optional[List[NamedTable]] = None,
    ) -> None:
        """Update (adds or overwrites) either a single table or a list of named tables in the repository dictionary.

        :param table: The single table to be updated in the
        repository dictionary.
        :param tables: The list of tables to be updated in
        the repository dictionary.
        :return:
        """
        if table:
            self.data.update({table.name: table})
        elif tables:
            [self.data.update({table.name: table}) for table in tables]
        else:
            pass

    def get_repo_summary(self) -> DataFrame:
        """Produce a summary table of the data repository content (table names, counts and columns).

        :return: The summary table as a dataframe.
        """
        data = [
            (
                table_name,
                f"{len(loaded_table.dataframe):,d}",
                list(loaded_table.dataframe),
            )
            for table_name, loaded_table in self.data.items()
        ]
        summary_df = pd.DataFrame(data, columns=SCHEMA_DATA_REPO_SUMMARY)
        return summary_df


@dataclass
class AlignmentStep:
    """Represent an alignment step metadata as a dataclass."""

    mapping_type_group: str
    source: str
    step_counter: int
    count_unmapped_nodes: int
    count_mappings: int
    count_nodes_one_source_to_many_target: int
    count_merged_nodes: int

    def __init__(
<<<<<<< Updated upstream
        self,
        mapping_type_group: str,
        source_id: str,
        step_counter: int,
        count_unmapped_nodes: int,
=======
        self, mapping_type_group: str, source: str, step_counter: int, count_unmapped_nodes: int,
>>>>>>> Stashed changes
    ):
        """Initialise the AlignmentStep dataclass.

        :param mapping_type_group: The mapping type group used
        in the alignment step.
        :param source: The ontology being aligned in this step.
        :param step_counter: The number of the step.
        :param count_unmapped_nodes: The number of unmapped nodes at
        the start of the alignment step.
        """
        self.mapping_type_group = mapping_type_group
        self.source = source
        self.step_counter = step_counter
        self.count_unmapped_nodes = count_unmapped_nodes
        self.count_mappings = 0
        self.count_nodes_one_source_to_many_target = 0
        self.count_merged_nodes = 0


@dataclass
class ConnectivityStep:
    """Hierarchy connectivity step metadata."""

    source_id: str
    count_unmapped_nodes: int
    count_reachable_unmapped_nodes: int
    count_available_edges: int
    count_produced_edges: int
    count_connected_nodes: int

    def __init__(self, source_id: str, count_unmapped_node_ids: int):
        """Initialise the ConnectivityStep dataclass.

        :param source_id: The node that are being connected in this step.
        :param count_unmapped_node_ids: The number of dangling and unmapped nodes
        of the ontology at the start of the connectivity step.
        """
        self.source_id = source_id
        self.count_unmapped_nodes = count_unmapped_node_ids
        self.count_reachable_unmapped_nodes = 0
        self.count_available_edges = 0
        self.count_produced_edges = 0
        self.count_connected_nodes = 0
