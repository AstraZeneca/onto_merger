from onto_merger.pipeline import Pipeline

EXAMPLE_DATASET = "../data/bikg_disease"


def main():
    Pipeline(project_folder_path=EXAMPLE_DATASET).run_alignment_and_connection_process()


if __name__ == "__main__":
    main()
