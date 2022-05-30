"""Helper methods to use Pandas profiling."""

from typing import List

from pandas_profiling import ProfileReport

from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import NamedTable
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


def profile_tables(tables: List[NamedTable], data_manager: DataManager) -> None:
    """Run the Pandas profiling process for a list of tables.

    :param tables: The tables to be profiled.
    :param data_manager: The data manager.
    :return:
    """
    table_names = [table.name for table in tables]
    logger.info(f"Starting Pandas profiling for {len(tables)} tables: '{table_names}'")
    for table in tables:
        logger.info(f"Profiling table '{table.name}'")
        report = produce_table_report(table=table)
        report.to_file(output_file=data_manager.get_profiled_table_report_path(table_name=table.name))
    logger.info(f"Finished Pandas profiling for tables '{table_names}'.")


def produce_table_report(table: NamedTable) -> ProfileReport:
    """Run the Pandas profiling process for one named table.

    :param table: The named table to be profiled.
    :return:
    """
    return ProfileReport(
        df=table.dataframe.reset_index(drop=True, inplace=False),
        title=f"{table.name} Profiling Report",
        html={"style": {"logo": "../images/onto_merger_logo.jpg"}},
        minimal=True,
        n_freq_table_max=250,
    )
