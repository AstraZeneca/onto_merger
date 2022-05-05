from onto_merger.pipeline import Pipeline

EXAMPLE_DATASET = "../data/bikg_2022-02-28-4.27.0_disease"


def main():
    Pipeline(project_folder_path=EXAMPLE_DATASET).run_alignment_and_connection_process()


if __name__ == "__main__":
    main()
