"""Class and helper methods for data loading and serialisation."""

import json
import os
import shutil
import typing
from pathlib import Path
from typing import List, Union

import pandas as pd
from pandas import DataFrame

from onto_merger.alignment import merge_utils
from onto_merger.data.constants import (
    DIRECTORY_ANALYSIS,
    DIRECTORY_DATA_TESTS,
    DIRECTORY_DOMAIN_ONTOLOGY,
    DIRECTORY_DROPPED_MAPPINGS,
    DIRECTORY_INPUT,
    DIRECTORY_INTERMEDIATE,
    DIRECTORY_LOGS,
    DIRECTORY_OUTPUT,
    DIRECTORY_PROFILED_DATA,
    DIRECTORY_REPORT,
    DOMAIN_SUFFIX,
    FILE_NAME_CONFIG_JSON,
    FILE_NAME_LOG,
    SCHEMA_EDGE_SOURCE_TO_TARGET_IDS,
    SCHEMA_HIERARCHY_EDGE_TABLE,
    SCHEMA_MAPPING_TABLE,
    SCHEMA_MERGE_TABLE_WITH_META_DATA,
    TABLE_EDGES_HIERARCHY,
    TABLE_EDGES_HIERARCHY_DOMAIN,
    TABLE_EDGES_HIERARCHY_POST,
    TABLE_MAPPINGS_DOMAIN,
    TABLE_MAPPINGS_UPDATED,
    TABLE_MERGES_AGGREGATED,
    TABLE_MERGES_WITH_META_DATA,
    TABLE_NODES,
    TABLE_NODES_MERGED,
    TABLES_INPUT,
    TABLES_INTERMEDIATE,
    TABLES_OUTPUT,
)
from onto_merger.data.dataclasses import (
    AlignmentConfig,
    AlignmentConfigBase,
    AlignmentConfigMappingTypeGroups,
    DataRepository,
    NamedTable,
)
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


class DataManager:
    """Performs file operations: loading and saving data frames, JSONs, and deleting files.

    Note: in the future this can be extend to split between Pandas and Spark operations.
    """

    def __init__(self, project_folder_path: str, clear_output_directory: bool = True):
        """Initialise the DataManager class.

        :param project_folder_path: The project folder path.
        """
        self._project_folder_path = project_folder_path
        if clear_output_directory is True:
            self._clear_output_directory()
        self._create_output_directory_structure()
        self.config = self.load_alignment_config()

    # CONFIG #
    def load_alignment_config(self) -> AlignmentConfig:
        """Parse the alignment configuration file into an AlignmentConfig dataclass.

        :return: The AlignmentConfig dataclass.
        """
        return DataManager.convert_config_json_to_dataclass(config_json=self._load_config_json())

    def _load_config_json(self) -> dict:
        """Load the alignment configuration JSON.

        :return: The JSON content as a dict.
        """
        file_path = os.path.join(self._project_folder_path, DIRECTORY_INPUT, FILE_NAME_CONFIG_JSON)
        with open(file_path) as json_file:
            config_json = json.load(json_file)
        logger.info(f"Loaded configuration JSON from '{file_path}'")
        return config_json

    @staticmethod
    @typing.no_type_check
    def convert_config_json_to_dataclass(config_json: dict) -> AlignmentConfig:
        """Convert alignment configuration dictionary to AlignmentConfig dataclass.

        :param config_json: The alignment configuration as a dictionary.
        :return: The AlignmentConfig dataclass.
        """
        image_format = "html"
        if "image_format" in config_json:
            image_format = config_json.get("image_format")
        mapping_config: dict = config_json["mappings"]
        alignment_config = AlignmentConfig(
            base_config=AlignmentConfigBase.from_dict(config_json),
            mapping_type_groups=AlignmentConfigMappingTypeGroups.from_dict(mapping_config["type_groups"]),
            as_dict=config_json,
            image_format=image_format
        )
        return alignment_config

    # DIRECTORY STRUCTURE #
    def _create_output_directory_structure(self) -> None:
        """Produce the empty directory structure for the output files.."""
        directory_paths = [
            self.get_analysis_folder_path(),
            self._get_profiled_report_directory_path(),
            self.get_data_tests_path(),
            self.get_dropped_mappings_path(),
            os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_REPORT, DIRECTORY_LOGS),
            self.get_domain_ontology_folder_path(),
        ]
        for directory_path in directory_paths:
            Path(directory_path).mkdir(parents=True, exist_ok=True)

    def _clear_output_directory(self) -> None:
        """Delete the output folder and its contents."""
        output_path = os.path.join(self._project_folder_path, DIRECTORY_OUTPUT)
        if os.path.exists(output_path):
            shutil.rmtree(output_path, ignore_errors=True)

    # LOADING #
    def load_table(self, table_name: str, process_directory: str) -> DataFrame:
        """Load a table as a data frame.

        :param process_directory: The process directory (input or output).
        :param table_name: The name of the table.
        :return: The loaded table.
        """
        file_path = self.get_table_path(process_directory=process_directory, table_name=table_name)
        df = pd.read_csv(file_path).drop_duplicates(keep="first", ignore_index=True)
        logger.info(f"Loaded table '{table_name}' with {len(df):,d} row(s).")
        return df

    def load_input_tables(self) -> List[NamedTable]:
        """Load the input csv-s into named tables.

        :return: The input named tables.
        """
        return [
            NamedTable(
                table_name,
                self.load_table(table_name=table_name, process_directory=DIRECTORY_INPUT),
            )
            for table_name in TABLES_INPUT
        ]

    def load_output_tables(self) -> List[NamedTable]:
        """Load the output csv-s into named tables.

        :return: The output named tables.
        """
        return [
            NamedTable(
                f"{table_name}{DOMAIN_SUFFIX}",
                self.load_table(table_name=table_name,
                                process_directory=f"{DIRECTORY_OUTPUT}/{DIRECTORY_DOMAIN_ONTOLOGY}"),
            )
            for table_name in TABLES_OUTPUT
        ]

    def load_intermediate_tables(self) -> List[NamedTable]:
        """Load the intermediate csv-s into named tables.

        :return: The intermediate named tables.
        """
        return [
            NamedTable(
                table_name,
                self.load_table(table_name=table_name,
                                process_directory=f"{DIRECTORY_OUTPUT}/{DIRECTORY_INTERMEDIATE}"),
            )
            for table_name in TABLES_INTERMEDIATE
        ]

    def load_specified_tables(self, table_names: List[str]) -> List[NamedTable]:
        """Load tables specified in the input.

        :param table_names: The list of tables to load.
        :return: The loaded named tables.
        """
        return [
            NamedTable(
                table_name,
                self.load_table(table_name=table_name,
                                process_directory=f"{DIRECTORY_OUTPUT}/{DIRECTORY_INTERMEDIATE}"),
            )
            for table_name in table_names
        ]

    def load_analysis_report_table_as_dict(self,
                                           section_name: str,
                                           table_name: str,
                                           rename_columns: dict = None) -> List[dict]:
        """Load a CSV table as a list of dictionaries.

        :param section_name: The name of the report section (prefix of the file).
        :param table_name: The name of the report section (suffix of the file).
        :param rename_columns: Column renaming dictionary.
        :return: The CSV loaded as a list of dictionaries.
        """
        # todo fix this hack
        df = self.load_analysis_report_table(section_name=section_name, table_name=table_name)
        if df is None:
            return []
        if rename_columns is not None:
            df.rename(columns=rename_columns, inplace=True)
        return [
            {
                col: row[col]
                for col in list(df)
            }
            for _, row in df.iterrows()
        ]

    def load_analysis_report_table(self, section_name: str, table_name: str) -> Union[DataFrame, None]:
        """Load an analysis report table.

        :param section_name: The name of the report section (prefix of the file).
        :param table_name: The name of the report section (suffix of the file).
        :return: The loaded table as a dataframe if the table exists, otherwise None.
        """
        file_name = f"{section_name}_{table_name}.csv"
        logger.info(f"load_analysis_report_table {os.path.join(self.get_analysis_folder_path(), file_name)}")
        file_path = os.path.join(self.get_analysis_folder_path(), file_name)
        try:
            df = pd.read_csv(file_path)
            return df
        except FileNotFoundError as e:
            logger.error(f"Data table missing: {e}")
        return None

    # SAVING #
    def save_table(
            self, table: NamedTable, process_directory: str = f"{DIRECTORY_OUTPUT}/{DIRECTORY_INTERMEDIATE}"
    ) -> None:
        """Save a given Pandas dataframe as a CSV.

        :return:
        """
        # only output tables are saved
        file_path = self.get_table_path(process_directory=process_directory, table_name=table.name)
        logger.info(f"Saving table '{f'{table.name}.csv'}' with {len(table.dataframe):,d} " + f"row(s) to {file_path}.")
        table.dataframe.to_csv(file_path, index=False)

    def save_tables(self, tables: List[NamedTable], process_directory: str = None) -> None:
        """Save a list of named tables Pandas dataframe part as CSVs.

        :param tables: The tables to be saved.
        :param process_directory: The process directory where the tables are saved to.
        :return:
        """
        for table in tables:
            if not process_directory:
                self.save_table(table=table)
            else:
                self.save_table(table=table, process_directory=process_directory)

    def save_domain_ontology_tables(self, tables: List[NamedTable]) -> None:
        """Save the domain ontology files.

        :param tables: The domain ontology named tables that we are saving.
        :return:
        """
        self.save_tables(
            tables=[
                NamedTable(name=table.name.replace(DOMAIN_SUFFIX, ""), dataframe=table.dataframe) for table in tables
            ],
            process_directory=f"{DIRECTORY_OUTPUT}/{DIRECTORY_DOMAIN_ONTOLOGY}",
        )

    def save_analysis_table(self,
                            analysis_table: DataFrame,
                            dataset: str,
                            analysed_table_name: str,
                            analysis_table_suffix: str,
                            index=False) -> None:
        """Save an analysis table.

        :param analysis_table: The analysis table we are saving.
        :param dataset: The name of the analysed dataset (used for forming the file path).
        :param analysed_table_name: The name of the analysis table (used for forming the file path).
        :param analysis_table_suffix: The suffix of the file path (analysis type).
        :param index: Save with index if True, otherwise save it without index.
        :return:
        """
        analysis_table.to_csv(
            os.path.join(
                self.get_analysis_folder_path(),
                f"{dataset}_{analysed_table_name}_{analysis_table_suffix}.csv"
            ),
            index=index
        )

    def save_analysis_named_tables(self,
                                   dataset: str,
                                   tables: List[NamedTable],
                                   index=False) -> None:
        """Save named analysis tables.

        :param dataset: The name of the analysed dataset (used for forming the file path).
        :param tables: The list of named tables we are saving.
        :param index: Save with index if True, otherwise save it without index.
        :return:
        """
        for table in tables:
            table.dataframe.to_csv(
                path_or_buf=os.path.join(
                    self.get_analysis_folder_path(),
                    f"{dataset}_{table.name}.csv"
                ),
                index=index
            )

    def save_dropped_mappings_table(
            self, table: DataFrame, step_count: int, source_id: str, mapping_type: str
    ) -> None:
        """Save a dropped mapping dataframe as a CSV, with meta data in the file name.

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

    def save_merged_ontology_report(self, content: str, template_search_path: str) -> str:
        """Save the analysis report HTML content.

        :param content: The report content as a string.
        :param template_search_path: The template path used to specify the report file path.
        :return: The saved report file path.
        """
        file_path = self.produce_analysis_report_path()
        with open(file_path, "w") as f:
            f.write(content)
        self._copy_analysis_images_and_report_assets(template_search_path=template_search_path)
        return file_path

    # COPY & MOVE #
    def _copy_analysis_images_and_report_assets(self, template_search_path: str) -> None:
        """Copy the images and analysis figures that are displayed in the HTML report.

        :param template_search_path: The template path used to specify the report file path.
        :return:
        """
        # images  from assets
        images_folder_to_path = os.path.join(self._produce_analysis_report_folder_path(), "images")

        if Path(images_folder_to_path).exists() is False:
            shutil.copytree(
                os.path.abspath(os.path.join(f"{template_search_path}/templates/images")),
                images_folder_to_path
            )
        # plots from analysis output folder
        [
            shutil.move(
                os.path.join(self.get_analysis_folder_path(), figure_file),
                os.path.join(images_folder_to_path, figure_file)
            )
            for figure_file in os.listdir(self.get_analysis_folder_path())
            if figure_file.endswith(f".{self.config.image_format}")
        ]

    def move_data_docs_to_reports(self) -> None:
        """Move the data doc files to the report folder."""
        from_path = os.path.join(self.get_data_tests_path(), "uncommitted/data_docs")
        to_path = os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_REPORT, "data_docs")
        shutil.copytree(from_path, to_path)

    # PATHS #
    def get_project_folder_path(self) -> str:
        """Produce the project folder absolute path.

        :return: The path as a string.
        """
        return str(self._project_folder_path)

    def get_input_folder_path(self) -> str:
        """Produce the input data folder absolute path.

        :return: The path as a string.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_INPUT)

    def get_domain_ontology_folder_path(self) -> str:
        """Produce the output data (i.e. domain ontology) folder absolute path.

        :return: The path as a string.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_DOMAIN_ONTOLOGY)

    def get_intermediate_folder_path(self) -> str:
        """Produce the intermediate data folder absolute path.

        :return: The path as a string.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_INTERMEDIATE)

    def get_ge_data_docs_folder_path(self):
        """Produce the data docs folder (post copy to reports) absolute path.

        :return: The path as a string.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_REPORT, "data_docs")

    def get_ge_data_docs_validations_folder_path(self):
        """Produce the data validation configuration JSON folder absolute path.

        :return: The path as a string.
        """
        return os.path.join(self.get_ge_data_docs_folder_path(), "local_site/validations")

    def get_ge_data_docs_index_path_for_input(self):
        """Produce the data docs HTML report main page absolute path.

        :return: The path as a string.
        """
        return os.path.join(self.get_data_tests_path(), "uncommitted/data_docs/local_site/index.html")

    def get_ge_json_validations_folder_path(self):
        """Produce the data validation result JSONs folder absolute path.

        :return: The path as a string.
        """
        return os.path.join(self.get_data_tests_path(), "uncommitted/validations")

    def get_output_report_folder_path(self):
        """Produce the HTML report folder absolute path.

        :return: The path as a string.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_REPORT)

    def get_analysis_folder_path(self):
        """Produce the analysis data folder absolute path.

        :return: The path as a string.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_INTERMEDIATE, DIRECTORY_ANALYSIS)

    def get_hierarchy_edges_paths_debug_file_path(self):
        """Produce the hierarchy edge debug file absolute path.

        :return: The path as a string.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_INTERMEDIATE, DIRECTORY_ANALYSIS,
                            "connectivity_hierarchy_edges_paths.csv")

    @staticmethod
    def get_absolute_path(path: str) -> str:
        """Return the absolute path for a path.

        :param path: The input path as a string.
        :return: The absolute pathas a string.
        """
        return os.path.abspath(path)

    def get_table_path(self, process_directory: str, table_name: str) -> str:
        """Produce a path (in the appropriate project sub folder) for a given table.

        :param process_directory: The process directory (input or output).
        :param table_name: The name of the table.
        :return: The project folder path of the given table.
        """
        return os.path.join(self._project_folder_path, process_directory, f"{table_name}.csv")

    def _get_profiled_report_directory_path(self) -> str:
        """Produce the path for the Pandas profile reports directory."""
        return os.path.join(
            self._project_folder_path,
            DIRECTORY_OUTPUT,
            DIRECTORY_REPORT,
            DIRECTORY_PROFILED_DATA,
        )

    def get_data_tests_path(self) -> str:
        """Produce the path for data test directory."""
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_INTERMEDIATE, DIRECTORY_DATA_TESTS)

    def get_domain_ontology_path(self) -> str:
        """Produce the path for the domain ontology folder."""
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_DOMAIN_ONTOLOGY)

    def get_dropped_mappings_path(self) -> str:
        """Produce the path for a dropped mapping."""
        return os.path.join(
            self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_INTERMEDIATE, DIRECTORY_DROPPED_MAPPINGS
        )

    def get_profiled_table_report_path(self, table_name: str, relative_path=False) -> str:
        """Produce the path for the Pandas profile report HTML."""
        table_html = f"{table_name}_report.html"
        if relative_path is True:
            return os.path.join(DIRECTORY_PROFILED_DATA, table_html)
        return os.path.join(self._get_profiled_report_directory_path(), table_html)

    def get_log_file_path(self) -> str:
        """Produce the path for log file."""
        return os.path.join(
            self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_REPORT, DIRECTORY_LOGS, FILE_NAME_LOG
        )

    @staticmethod
    def _get_analysis_figure_file_name(dataset: str,
                                       analysed_table_name: str,
                                       analysis_table_suffix: str) -> str:
        return f"{dataset}_{analysed_table_name}_{analysis_table_suffix}"

    def get_analysis_figure_path(self,
                                 dataset: str,
                                 analysed_table_name: str,
                                 analysis_table_suffix: str) -> str:
        """Produce the path for an analysis figure.

        :param dataset:
        :param analysed_table_name:
        :param analysis_table_suffix:
        :return:
        """
        file_name = self._get_analysis_figure_file_name(
            dataset=dataset,
            analysed_table_name=analysed_table_name,
            analysis_table_suffix=analysis_table_suffix
        )
        file_name_with_type = f"{file_name}.{self.config.image_format}"
        return os.path.join(
            self.get_analysis_folder_path(),
            file_name_with_type,
        )

    def _produce_analysis_report_folder_path(self) -> str:
        """Produce the analysis report folder path.

        :return: The analysis report folder path.
        """
        return os.path.join(self._project_folder_path, DIRECTORY_OUTPUT, DIRECTORY_REPORT)

    def produce_analysis_report_path(self) -> str:
        """Produce the analysis report HTML path.

        :return: The report HTML path.
        """
        return os.path.join(self._produce_analysis_report_folder_path(), "index.html")

    # TABLES #
    @staticmethod
    def merge_tables_of_same_type(tables: List[NamedTable]) -> NamedTable:
        """Merge a list of named tables (that must be the same type) into a single named table.

        :param tables: The list of named tables.
        :return: The merged named table.
        """
        return NamedTable(
            tables[0].name,
            pd.concat([table.dataframe for table in tables]).drop_duplicates(keep="first"),
        )

    @staticmethod
    def produce_empty_merge_table() -> NamedTable:
        """Produce an empty named merge table.

        :return: The merged named table.
        """
        return NamedTable(
            name=TABLE_MERGES_WITH_META_DATA,
            dataframe=pd.DataFrame([], columns=list(SCHEMA_MERGE_TABLE_WITH_META_DATA)),
        )

    @staticmethod
    def produce_empty_hierarchy_table() -> NamedTable:
        """Produce an empty hierarchy edge table.

        :return: The empty hierarchy named table.
        """
        return NamedTable(
            name=TABLE_EDGES_HIERARCHY,
            dataframe=pd.DataFrame([], columns=SCHEMA_HIERARCHY_EDGE_TABLE),
        )

    @staticmethod
    def produce_domain_ontology_tables(data_repo: DataRepository) -> List[NamedTable]:
        """Produce the domain ontology files with minimal data.

        Tables used in the pipeline may contain inferred column values, e.g. node ID
         namespaces and process (alignment and connectivity) meta data.

        :param data_repo: The data repository containing the files.
        :return: The finalised (i.e. domain ontology) tables.
        """
        return [
            merge_utils.produce_named_table_domain_nodes(
                nodes=data_repo.get(TABLE_NODES).dataframe,
                merged_nodes=data_repo.get(TABLE_NODES_MERGED).dataframe,
            ),
            merge_utils.produce_named_table_domain_merges(
                merges_aggregated=data_repo.get(TABLE_MERGES_AGGREGATED).dataframe
            ),
            NamedTable(
                name=TABLE_MAPPINGS_DOMAIN,
                dataframe=(
                    data_repo.get(TABLE_MAPPINGS_UPDATED).dataframe[SCHEMA_MAPPING_TABLE].sort_values(
                        by=SCHEMA_EDGE_SOURCE_TO_TARGET_IDS, ascending=True, inplace=False
                    )
                ),
            ),
            NamedTable(
                name=TABLE_EDGES_HIERARCHY_DOMAIN,
                dataframe=(
                    data_repo.get(TABLE_EDGES_HIERARCHY_POST).dataframe[SCHEMA_HIERARCHY_EDGE_TABLE].sort_values(
                        by=SCHEMA_HIERARCHY_EDGE_TABLE, ascending=True, inplace=False
                    )
                ),
            ),
        ]

    @staticmethod
    def get_file_system_loader_path() -> str:
        """Return the file system loader path according to the run environment (testing or live).

        :return: The file system loader path
        """
        if "tox.ini" in os.listdir("."):
            return "onto_merger/report"
        return "../../onto_merger/onto_merger/report"
