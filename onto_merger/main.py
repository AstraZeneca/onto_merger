from onto_merger.pipeline import Pipeline

EXAMPLE_DATASET = "../data/bikg_disease"
EXAMPLE_DATASET_LIGHT = "../tests/test_data"


def main():
    Pipeline(project_folder_path=EXAMPLE_DATASET_LIGHT).run_alignment_and_connection_process()


if __name__ == "__main__":
    main()
