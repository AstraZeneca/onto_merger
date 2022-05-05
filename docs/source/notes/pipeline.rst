Pipeline
=======================

The pipeline provides the main interface for the API and runs all input and
output validation, alignment and connectivity, and reporting processes to
produce the merged ontology.

Run
-------

Run from command line:

.. code-block:: shell

    $ onto_merger --f PROJECT_FOLDER

Run as Python code:

.. code-block:: python

    from onto_merger.pipeline import Pipeline

    # initialise the pipeline
    pipeline = Pipeline(project_folder_path="../path/to/project")

    # run the process
    pipeline.run_alignment_and_connection_process()


Steps
-------

The main pipeline steps are the following:


#. | **Config validation**: the alignment configuration JSON schema is
   | validated. If the schema is invalid the process will stop. Any errors are
   | displayed on the console and can also be found in the log file
   | (``PROJECT_FOLDER/output/report/log/onto-merger.logger``).
#. | **Input data loading and validation**: input tables are loaded and
   | preprocessed for profiling and validation. Tables are profiled
   | (``PROJECT_FOLDER/output/report/data_profile_reports/nodes.html``
   | etc), and data tested. Errors are displayed on the console and can be
   | viewed in detail in the data test documentation
   | (``PROJECT_FOLDER/output/report/data_docs/index.html``).
#. | **Alignment process**: the :doc:`alignment` process is run and the merges
   | are produced; these merges may contain non canonical IDs
   | (``PROJECT_FOLDER/output/intermediate/merges_temp.csv``).
#. | **Merge aggregation**: the merges are aggregated so each merge contains
   | the canonical node ID for each cluster
   | (``PROJECT_FOLDER/output/intermediate/merges.csv``).
#. | **Connectivity process**: the :doc:connectivity` process is run and the
   | single merged hierarchy is produced using the seed ontology hierarchy and
   | the other available hierarchies. This contains the seed ontology and the
   | unmapped nodes.
#. | **Output finalisation**: the final merged ontology tables are produced
   | (``PROJECT_FOLDER/output/domain_ont`` analysis (e.g. unmapped, dangling
   | nodes in ``PROJECT_FOLDER/output/intermediate/...``).
#. | **Output validation and profiling**: all output tables (intermediate as
   | well as final) are profiled for analysis, and the output tables are
   | data tested.
#. | **Alignment analysis report** is produced, providing summary and detail
   | of the various steps.
