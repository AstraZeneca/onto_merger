[pypi-image]: https://badge.fury.io/py/onto_merger.svg
[pypi-url]: https://pypi.python.org/pypi/onto_merger
[size-image]: https://img.shields.io/github/repo-size/AstraZeneca/onto_merger.svg
[size-url]: https://github.com/AstraZeneca/onto_merger/archive/main.zip
[build-image]: https://github.com/AstraZeneca/onto_merger/workflows/CI/badge.svg
[build-url]: https://github.com/AstraZeneca/onto_merger/actions?query=workflow%3ACI
[docs-image]: https://readthedocs.org/projects/ontomerger/badge/?version=latest
[docs-url]: https://ontomerger.readthedocs.io/en/latest/?badge=latest
[coverage-image]: https://codecov.io/gh/AstraZeneca/onto_merger/branch/main/graph/badge.svg
[coverage-url]: https://codecov.io/github/AstraZeneca/onto_merger?branch=main

<p align="center">
  <img width="90%" src="https://github.com/AZ-AI/onto_merger/blob/main/images/onto_merger_logo.jpg?sanitize=true" />
</p>

--------------------------------------------------------------------------------

[![PyPI Version][pypi-image]][pypi-url]
[![Docs Status][docs-image]][docs-url]
[![Code Coverage][coverage-image]][coverage-url]
[![Build Status][build-image]][build-url]
[![Arxiv](https://img.shields.io/badge/ArXiv-2202.05240-orange.svg)]()

**[Documentation](https://ontomerger.readthedocs.io)** | **[External Resources](https://ontomerger.readthedocs.io/en/latest/notes/resources.html)**

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
   author = {Geleta, David and Nikolov, Andriy and O'Donoghue, Mark and Rozemberczki, Benedek 
             and Gogleva, Anna and Tamma, Valentina and Payne, Terry R.},
   month = {june},
   title = {{OntoMerger: An Ontology Integration Library for Deduplicating and Connecting Knowledge Graph Nodes.}},
   url = {https://arxiv.org/abs/2206.02238},
   year = {2022}
}
```
--------------------------------------------------------------------------------

**Getting Started**

The API of `onto_merger` ...

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

- [Apache 2.0 License](https://github.com/AstraZeneca/onto_merger/blob/main/LICENSE)

--------------------------------------------------------------------------------

**Credit**

The **Onto Merger** logo is based on:

- [Galguna Font](https://www.dafont.com/galguna.font)
- [Noun Project Icons](https://thenounproject.com/)
