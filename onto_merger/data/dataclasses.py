from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from dataclasses_json import dataclass_json
from pandas import DataFrame

from onto_merger.data.constants import (
    INPUT_TABLES,
    OUTPUT_TABLES,
    SCHEMA_DATA_REPO_SUMMARY,
)


@dataclass_json
@dataclass
class AlignmentConfigMappingTypeGroups:
    """Represent the mapping type groups."""

    equivalence: List[str]
    database_reference: List[str]
    label_match: List[str]

    @property
    def all_mapping_types(self) -> List[str]:
        """Produces a single list containing all mapping relations.

        :return: All mappings relations.
        """
        return self.equivalence + self.database_reference + self.label_match


@dataclass_json
@dataclass
class AlignmentConfigBase:
    """Represents the base alignment process configuration."""

    domain_node_type: str
    seed_ontology_name: str


@dataclass
class AlignmentConfig:
    """Represents the alignment process configuration"""

    base_config: AlignmentConfigBase
    mapping_type_groups: AlignmentConfigMappingTypeGroups
    as_dict: dict


@dataclass
class NamedTable:
    """Wraps a Pandas dataframe with its name (identifier) for convenient access
    and serialisation."""

    name: str
    dataframe: DataFrame


class DataRepository:
    """Stores named tables in a dictionary and provides access and update
    convenience methods."""

    def __init__(self):
        """Initialised the DataRepository dataclass."""
        self.data: Dict[str, NamedTable] = {}

    def get(self, table_name: str) -> NamedTable:
        """Returns a named table for a given table identifier.

        :param table_name: The table identifier.
        :return: The named table.
        """
        table = self.data.get(table_name)
        if table is None:
            raise Exception
        else:
            return table

    def get_input_tables(self) -> List[NamedTable]:
        """Returns the list of input named tables.

        :return: The list of input named tables.
        """
        return [self.get(table_name=table_name) for table_name in INPUT_TABLES if table_name in self.data]

    def get_output_tables(self) -> List[NamedTable]:
        """Returns the list of output named tables.

        :return: The list of output named tables.
        """
        return [self.get(table_name=table_name) for table_name in OUTPUT_TABLES if table_name in self.data]

    def update(
        self,
        table: Optional[NamedTable] = None,
        tables: Optional[List[NamedTable]] = None,
    ) -> None:
        """Updates (adds or overwrites) either a single table or a list
        of named tables in the repository dictionary.

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
        """Produces a summary table of the data repository content
        (table names, counts and columns).

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
    """Represents an alignment step metadata as a dataclass."""

    mapping_type_group: str
    source_id: str
    step_counter: int
    count_unmapped_nodes: int
    count_mappings: int
    count_nodes_one_source_to_many_target: int
    count_merged_nodes: int

    def __init__(
        self,
        mapping_type_group: str,
        source_id: str,
        step_counter: int,
        count_unmapped_nodes: int,
    ):
        """Initialises the AlignmentStep dataclass.

        :param mapping_type_group: The mapping type group used
        in the alignment step.
        :param source_id: The ontology being aligned in the step.
        :param step_counter: The number of the step.
        :param count_unmapped_nodes: The number of unmapped nodes at
        the start of the alignment step.
        """
        self.mapping_type_group = mapping_type_group
        self.source_id = source_id
        self.step_counter = step_counter
        self.count_unmapped_nodes = count_unmapped_nodes
        self.count_mappings = 0
        self.count_nodes_one_source_to_many_target = 0
        self.count_merged_nodes = 0
