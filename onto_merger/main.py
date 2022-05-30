"""OntoMerger is an ontology alignment library for deduplicating and connecting knowledge graph nodes.

Knowledge graph nodes, i.e. ontology concepts, must represent the same domain, e.g. diseases.
After deduplication the nodes are connected to form a single directed acyclic hierarchical graph (DAG),
i.e. an ontology class hierarchy.

Usage:
    main.py -f <FOLDER_PATH>
    main.py -f EXAMPLE_DATASET
    main.py -f EXAMPLE_DATASET_LIGHT
    main.py (-h | --help)
    main.py -v

Arguments:
    <FOLDER_PATH>           The project folder where the inputs are provided and the outputs will be stored.
    EXAMPLE_DATASET         Example data set included in the project.
    EXAMPLE_DATASET_LIGHT   Another example data set, subset of the former, included in the project.

Options:
  -h --help         Show this screen.
  -f <FOLDER_PATH>  Run the OntoMerger alignemnt and connectivity process on the specified dataset.
  -v                Show version.

"""

from docopt import docopt

from onto_merger.pipeline import Pipeline
from onto_merger.version import __version__

example_data_sets = {"EXAMPLE_DATASET": "../data/bikg_disease", "EXAMPLE_DATASET_LIGHT": "../tests/test_data"}
FOLDER_PATH_ARG = "-f"
VERSION_ARG = "-v"


def main(project_folder_path: str) -> None:
    """Run the OntoMerger pipeline for the specified data set.

    :param project_folder_path: The data set path.
    :return:
    """
    Pipeline(project_folder_path=project_folder_path).run_alignment_and_connection_process()


if __name__ == "__main__":
    arguments = docopt(__doc__, version=f"OntoMerger v. {__version__}")
    if arguments[VERSION_ARG]:
        print(f"OntoMerger v. {__version__}")
    elif arguments[FOLDER_PATH_ARG]:
        if arguments[FOLDER_PATH_ARG] in example_data_sets:
            main(project_folder_path=example_data_sets[arguments[FOLDER_PATH_ARG]])
        else:
            main(project_folder_path=arguments[FOLDER_PATH_ARG])

# # todo
#
# import os
# from typing import List
# from onto_merger.report import report_generator
# from onto_merger.analyser.report_analyser import ReportAnalyser
# from onto_merger.data.data_manager import DataManager
# from onto_merger.data.dataclasses import DataRepository, RuntimeData
# from onto_merger.logger.log import setup_logger
#
# project_folder_path = os.path.abspath(
#     "/Users/kmnb265/Desktop/ONTOMERGE_Data/bikg_2022-02-28-4.27.0_disease_full_background_knowledge")
# analysis_data_manager = DataManager(project_folder_path=project_folder_path,
#                                     clear_output_directory=False)
# setup_logger(
#     module_name="test",
#     file_name=analysis_data_manager.get_log_file_path()
# )
# this_data_repo = DataRepository()
# this_data_repo.update(tables=analysis_data_manager.load_input_tables())
# this_data_repo.update(tables=analysis_data_manager.load_output_tables())
# this_data_repo.update(tables=analysis_data_manager.load_intermediate_tables())
# print(this_data_repo.get_repo_summary())
#
#
# def convert_runtime_data() -> List[RuntimeData]:
#     rd = analysis_data_manager.load_table("pipeline_steps_report", "output/intermediate")
#     rdl = [
#         RuntimeData(
#             task=r["task"],
#             start=r["start"],
#             end=r["end"],
#             elapsed=r["elapsed"],
#         )
#         for _, r in rd.iterrows()
#     ][0:-1]
#     return rdl
#
#
# ra = ReportAnalyser(
#     alignment_config=analysis_data_manager.load_alignment_config(),
#     data_repo=this_data_repo,
#     data_manager=analysis_data_manager,
#     runtime_data=convert_runtime_data(),
# )
#
# ra.produce_report_data()
# report_generator.produce_report(data_manager=analysis_data_manager)
