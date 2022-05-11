"""OntoMerger is an ontology alignment library for deduplicating knowledge graph nodes,
i.e. ontology concepts, that represent the same domain, e.g. diseases, and connecting
them to form a single directed acyclic hierarchical graph (DAG), i.e. an ontology class hierarchy.

Usage:
    main.py -f <FOLDER_PATH>
    main.py -f EXAMPLE_DATASET
    main.py -f EXAMPLE_DATASET_LIGHT
    main.py -h | --help
    main.py -v

Options:
  -h --help     Show this screen.
  -f FOLDER     Specify the project folder where the inputs are provided and the outputs will be stored.
  -v            Show the OntoMerger version.

"""

from docopt import docopt

from onto_merger.pipeline import Pipeline
from onto_merger.version import __version__

example_data_sets = {
    "EXAMPLE_DATASET": "../data/bikg_disease",
    "EXAMPLE_DATASET_LIGHT": "../tests/test_data"
}
FOLDER_PATH_ARG = "-f"


def main(project_folder_path: str) -> None:
    Pipeline(project_folder_path=project_folder_path).run_alignment_and_connection_process()


if __name__ == "__main__":
    arguments = docopt(__doc__, version=f'OntoMerger v. {__version__}')
    print(arguments)
    if arguments[FOLDER_PATH_ARG] in example_data_sets:
        main(project_folder_path=example_data_sets[arguments[FOLDER_PATH_ARG]])
    else:
        main(project_folder_path=arguments[FOLDER_PATH_ARG])
