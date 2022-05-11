import json
import os
import shutil
import typing
from pathlib import Path
from typing import List

import pandas as pd
from pandas import DataFrame

from onto_merger.data.constants import (
    CONFIG_JSON,
    DIRECTORY_DATA_TESTS,
    DIRECTORY_DROPPED_MAPPINGS,
    DIRECTORY_INPUT,
    DIRECTORY_OUTPUT,
    DIRECTORY_PROFILED_DATA,
    DIRECTORY_REPORT,
    FILE_NAME_LOG,
    INPUT_TABLES,
    SCHEMA_HIERARCHY_EDGE_TABLE,
    SCHEMA_MERGE_TABLE,
    TABLE_EDGES_HIERARCHY,
    TABLE_MERGES,
)
from onto_merger.data.dataclasses import (
    AlignmentConfig,
    AlignmentConfigBase,
    AlignmentConfigMappingTypeGroups,
    NamedTable,
)
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


class DataManager:
    """Provides methods for performing file operations: loading and saving data frames,
    JSONs, and deleting files.

    Note: in the future this can be extend to split between Pandas and Spark operations.
    """

    def __init__(self, project_folder_path: str, clear_output_directory: bool = True):
        """Initialises the DataManager class.

        :param project_folder_path: The project folder path.
        """
        self._project_folder_path = project_folder_path
        if clear_output_directory is True:
            self._clear_output_directory()
        self._create_output_directory_structure()

    def load_alignment_config(self) -> AlignmentConfig:
        """Parses the alignment configuration file into an AlignmentConfig dataclass.

        :return: The AlignmentConfig dataclass.
        """
        return DataManager.convert_config_json_to_dataclass(config_json=self._load_config_json())

    def _load_config_json(self) -> dict:
        """Loads the alignment configuration JSON.

        :return: The JSON content as a dict.
        """
        file_path = os.path.join(self._project_folder_path, DIRECTORY_INPUT, CONFIG_JSON)
        with open(file_path) as json_file:
            config_json = json.load(json_file)
        logger.info(f"Loaded configuration JSON from '{file_path}'")
        return config_json

    @staticmethod
    @typing.no_type_check
    def convert_config_json_to_dataclass(config_json: dict) -> AlignmentConfig:
        """Converts alignment configuration dictionary to AlignmentConfig dataclass.

        :param config_json: The alignment configuration as a dictionary.
        :return: The AlignmentConfig dataclass.
        """
        mapping_config: dict = config_json["mappings"]
        alignment_config = AlignmentConfig(
            base_config=AlignmentConfigBase.from_dict(config_json),
            mapping_type_groups=AlignmentConfigMappingTypeGroups.from_dict(mapping_config["type_groups"]),
            as_dict=config_json,
        )
        return alignment_config

    def load_table(self, table_name: str, process_directory: str) -> DataFrame:
        """Loads a table as a data frame.

        :param process_directory: The process directory (input or output).
        :param table_name: The name of the table.
        :return: The loaded table.
        """
        file_path = self.get_table_path(process_directory=process_directory, table_name=table_name)
        df = pd.read_csv(file_path).drop_duplicates(keep="first", ignore_index=True)
        logger.info(f"Loaded table '{table_name}' with {len(df):,d} row(s).")
        return df

    def load_input_tables(self) -> List[NamedTable]:
        """Loads the input csv-s into named tables.

        :return: The input named tables.
        """
        return [
            NamedTable(
                table_name,
                self.load_table(table_name=table_name, process_directory=DIRECTORY_INPUT),
            )
            for table_name in INPUT_TABLES
        ]

    @staticmethod
    def get_absolute_path(path: str) -> str:
        """Returns the absolute path for a path."""
        return os.path.abspath(path)

    def get_table_path(self, process_directory: str, table_name: str) -> str:
        """Produces a path (in the appropriate project sub folder) for a given table.

        :param process_directory: The process directory (input or output).
        :param table_name: The name of the table.
        :return: The project folder path of the given table.
        """
        return os.path.join(self._project_folder_path, process_directory, f"{table_name}.csv")

    def _get_profiled_report_directory_path(self) -> str:
        """Produces the path for the Pandas profile reports directory."""
        return os.path.join(
            self._project_folder_path,
            DIRECTORY_OUTPUT,
            DIRECTORY_REPORT,
            DIRECTORY_PROFILED_DATA,
        )

    def get_data_tests_path(self) -> str:
        """Produces the path for data test directory."""
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_DATA_TESTS)

    def get_dropped_mappings_path(self) -> str:
        """Produces the path for a dropped mapping."""
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_DROPPED_MAPPINGS)

    def get_profiled_table_report_path(self, table_name: str, relative_path=False) -> str:
        """Produces the path for the Pandas profile report HTML."""
        table_html = f"{table_name}_report.html"
        if relative_path is True:
            return os.path.join(DIRECTORY_PROFILED_DATA, table_html)
        return os.path.join(self._get_profiled_report_directory_path(), table_html)

    def get_log_file_path(self) -> str:
        """Produces the path for log file."""
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, FILE_NAME_LOG)

    def _create_output_directory_structure(self):
        """Produces the empty directory structure for the output files.."""
        directory_paths = [
            self._get_profiled_report_directory_path(),
            self.get_data_tests_path(),
            self.get_dropped_mappings_path(),
        ]
        for directory_path in directory_paths:
            Path(directory_path).mkdir(parents=True, exist_ok=True)

    def _clear_output_directory(self):
        """Deletes the output folder and its contents."""
        output_path = os.path.join(self._project_folder_path, DIRECTORY_OUTPUT)
        if os.path.exists(output_path):
            shutil.rmtree(output_path)

    def save_table(self, table: NamedTable) -> None:
        """Saves a given Pandas dataframe as a CSV."""
        # only output tables are saved
        file_path = self.get_table_path(process_directory=DIRECTORY_OUTPUT, table_name=table.name)
        logger.info(f"Saving table '{f'{table.name}.csv'}' with {len(table.dataframe):,d} " + f"row(s) to {file_path}.")
        table.dataframe.to_csv(file_path, index=False)

    def save_tables(self, tables: List[NamedTable]) -> None:
        """Saves a list of named tables Pandas dataframe part as CSVs."""
        for table in tables:
            self.save_table(table=table)

    def save_merged_ontology_report(self, content) -> str:
        """Saves the analysis report HTML content."""
        file_path = self.produce_merged_ontology_report_path()
        with open(file_path, "w") as f:
            f.write(content)
        return file_path

    def produce_merged_ontology_report_path(self):
        """Produces the analysis report HTML path."""
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_REPORT, "index.html")

    def save_dropped_mappings_table(self, table: DataFrame, step_count: int, source_id: str, mapping_type: str) -> None:
        """Saves a dropped mapping dataframe as a CSV, with meta data in the file name.

        :param table: The dataframe to be saved.
        :param step_count: The alignment step number.
        :param source_id: The aligned source name.
        :param mapping_type: The type of mappings in the table.
        :return:
        """
        if len(table) > 0:
            table.to_csv(
                path_or_buf=os.path.join(
                    self.get_dropped_mappings_path(),
                    f"{mapping_type}_{str(step_count)}_{source_id}.csv",
                ),
                index=False,
            )

    @staticmethod
    def merge_tables_of_same_type(tables: List[NamedTable]) -> NamedTable:
        """Merges a list of named tables (that must be the same type) into a single
        named table.

        :param tables: The list of named tables.
        :return:
        """
        return NamedTable(
            tables[0].name,
            pd.concat([table.dataframe for table in tables]).drop_duplicates(keep="first"),
        )

    @staticmethod
    def produce_empty_merge_table() -> NamedTable:
        """Produces an empty named merge table."""
        return NamedTable(name=TABLE_MERGES, dataframe=pd.DataFrame([], columns=SCHEMA_MERGE_TABLE))

    @staticmethod
    def produce_empty_hierarchy_table() -> NamedTable:
        """Produces an empty hierarchy edge table."""
        return NamedTable(
            name=TABLE_EDGES_HIERARCHY,
            dataframe=pd.DataFrame([], columns=SCHEMA_HIERARCHY_EDGE_TABLE),
        )
