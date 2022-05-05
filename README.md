
--------------------------------------------------------------------------------

*OntoMerger* is an ontology alignment library for **deduplicating** knowledge
graph nodes,(i.e. ontology concepts) that represent the *same domain*, e.g. diseases,
and **connecting** them to form a single directed acyclic hierarchical graph (DAG) (i.e. an ontology
class hierarchy).  The library implements a pipeline that takes *nodes, mappings and
(disconnected) hierarchies* as input and produces *node merges* and a *connected hierarchy*.
It also provides analysis and data testing for fine tuning the inputs in order
to further reduce duplication, as well as to increase connectivity.

--------------------------------------------------------------------------------

**Citing**


If you find *OntoMerger* useful in your work or research, please consider adding the following citation:

```bibtex
@article{ontomerger,
   arxivId = {???},
   author = {Geleta, David and Rozemberczki, Benedek and Nikolov, Andriy and O'Donoghue, Mark and Gogleva, Anna and Tamma, Valentina},
   month = {may},
   title = {{OntoMerger: An Ontology Alignment Library for
             Creating Minimal and Connected Domain Knowledge
             Sub-graphs.}},
   url = {http://arxiv.org/abs/???},
   year = {2022}
}
```
--------------------------------------------------------------------------------

**Getting Started**

The API of `chemicalx` provides a high-level function for training and evaluating models
that's heavily influenced by the [PyKEEN](https://github.com/pykeen/pykeen/)
training and evaluation pipeline:

```python

from onto_merger.pipeline import Pipeline

# initialise the pipeline
pipeline = Pipeline(project_folder_path="../path/to/project")

# run the process
pipeline.run_alignment_and_connection_process()

# view results in "../path/to/project/output/report/index.html"
```

--------------------------------------------------------------------------------

**Running tests**

```
$ tox -e py
```
--------------------------------------------------------------------------------

**License**

- [Apache 2.0 License](https://github.com/AstraZeneca/chemicalx/blob/main/LICENSE)
