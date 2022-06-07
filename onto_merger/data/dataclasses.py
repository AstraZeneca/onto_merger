"""Data classes and helper methods."""

import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from dataclasses_json import dataclass_json
from pandas import DataFrame

from onto_merger.data.constants import (
    SCHEMA_ALIGNMENT_STEPS_TABLE,
    SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE,
    SCHEMA_DATA_REPO_SUMMARY,
    SCHEMA_PIPELINE_STEPS_REPORT_TABLE,
    TABLE_ALIGNMENT_STEPS_REPORT,
    TABLE_CONNECTIVITY_STEPS_REPORT,
    TABLE_PIPELINE_STEPS_REPORT,
    TABLES_DOMAIN,
    TABLES_INPUT,
    TABLES_INTERMEDIATE,
)


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
        """Get the mapping type group name list.

        :return: The mapping type group name list.
        """
        return [str(k) for k in self.__dict__.keys()]


@dataclass_json
@dataclass
class AlignmentConfigBase:
    """Base alignment process configuration."""

    domain_node_type: str
    seed_ontology_name: str
    force_through_failed_validation: bool = False


@dataclass
class AlignmentConfig:
    """Alignment process configuration."""

    base_config: AlignmentConfigBase
    mapping_type_groups: AlignmentConfigMappingTypeGroups
    as_dict: dict
    image_format: str


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


@dataclass_json
@dataclass
class RuntimeData:
    """Represent an pipeline step metadata as a dataclass."""

    task: str
    start: str
    end: str
    elapsed: float

    def __init__(
            self,
            task: str,
            start: str,
            end: str,
            elapsed: float,
    ):
        """Initialise the RuntimeData dataclass.

        :param task: The task name.
        :param start: The task start date time string.
        :param end: The task end date time string.
        :param elapsed: The task elapsed seconds.
        """
        self.task = task
        self.start = start
        self.end = end
        self.elapsed = elapsed


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
    task: str
    start: str
    start_date_time: datetime
    end: str
    elapsed: float

    def __init__(
            self,
            mapping_type_group: str,
            source: str,
            step_counter: int,
            count_unmapped_nodes: int,
    ) -> None:
        """Initialise the AlignmentStep dataclass.

        :param mapping_type_group: The mapping type group used
        in the alignment step.
        :param source: The ontology being aligned in this step.
        :param step_counter: The number of the step.
        :param count_unmapped_nodes: The number of unmapped nodes at
        the start of the alignment step.
        """
        self.start_date_time = datetime.now()
        self.start = format_datetime(date_time=self.start_date_time)
        self.task = f"Aligning {source} {mapping_type_group}"
        self.mapping_type_group = mapping_type_group
        self.source = source
        self.step_counter = step_counter
        self.count_unmapped_nodes = count_unmapped_nodes
        self.count_mappings = 0
        self.count_nodes_one_source_to_many_target = 0
        self.count_merged_nodes = 0
        self.end = ""
        self.elapsed = 0

    def task_finished(self) -> None:
        """Stop the task runtime counter.

        :return:
        """
        end = datetime.now()
        self.end = format_datetime(date_time=end)
        self.elapsed = (end - self.start_date_time).total_seconds()


@dataclass
class ConnectivityStep:
    """Hierarchy connectivity step metadata."""

    step_counter: int
    source_id: str
    count_unmapped_nodes: int
    count_reachable_unmapped_nodes: int
    count_available_edges: int
    count_produced_edges: int
    count_connected_nodes: int
    task: str
    start: str
    start_date_time: datetime
    end: str
    elapsed: float

    def __init__(self, source_id: str, count_unmapped_node_ids: int):
        """Initialise the ConnectivityStep dataclass.

        :param source_id: The node that are being connected in this step.
        :param count_unmapped_node_ids: The number of dangling and unmapped nodes
        of the ontology at the start of the connectivity step.
        """
        self.start_date_time = datetime.now()
        self.start = format_datetime(date_time=self.start_date_time)
        self.source_id = source_id
        self.task = f"Connecting {source_id}"
        self.count_unmapped_nodes = count_unmapped_node_ids
        self.count_reachable_unmapped_nodes = 0
        self.count_available_edges = 0
        self.count_produced_edges = 0
        self.count_connected_nodes = 0
        self.end = ""
        self.elapsed = 0

    def task_finished(self) -> None:
        """Stop the task runtime counter.

        :return:
        """
        end = datetime.now()
        self.end = format_datetime(date_time=end)
        self.elapsed = (end - self.start_date_time).total_seconds()


def convert_runtime_steps_to_named_table(
        steps: List[RuntimeData],
) -> NamedTable:
    """Convert the runtime data to a named table.

    :param steps: The list of runtime step objects.
    :return: The runtime data named table.
    """
    return NamedTable(
        TABLE_PIPELINE_STEPS_REPORT,
        pd.DataFrame(
            [dataclasses.astuple(step) for step in steps],
            columns=SCHEMA_PIPELINE_STEPS_REPORT_TABLE,
        ),
    )


def convert_alignment_steps_to_named_table(
        alignment_steps: List[AlignmentStep],
) -> NamedTable:
    """Convert the list of AlignmentStep dataclasses to a named table.

    :param alignment_steps: The list of AlignmentStep dataclasses.
    :return: The AlignmentStep report dataframe wrapped as a named table.
    """
    return NamedTable(
        TABLE_ALIGNMENT_STEPS_REPORT,
        pd.DataFrame(
            [dataclasses.astuple(alignment_step) for alignment_step in alignment_steps],
            columns=SCHEMA_ALIGNMENT_STEPS_TABLE,
        ),
    )


def convert_connectivity_steps_to_named_table(
        steps: List[ConnectivityStep],
) -> NamedTable:
    """Convert the list of ConnectivityStep dataclasses to a named table.

    :param steps: The list of ConnectivityStep dataclasses.
    :return: The ConnectivityStep report dataframe wrapped as a named table.
    """
    return NamedTable(
        TABLE_CONNECTIVITY_STEPS_REPORT,
        pd.DataFrame(
            [dataclasses.astuple(step) for step in steps],
            columns=SCHEMA_CONNECTIVITY_STEPS_REPORT_TABLE,
        ),
    )


def format_datetime(date_time: datetime) -> str:
    """Format a date time to string.

    :param date_time: The date time.
    :return: The formatted date time as a string.
    """
    return f"{date_time.strftime('%Y-%m-%d %H:%M:%S')}"
