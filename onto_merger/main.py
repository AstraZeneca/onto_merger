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
