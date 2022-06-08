"""Setup the package."""

import os

from setuptools import find_packages, setup

on_rtd = os.environ.get("READTHEDOCS") == "True"

install_requires = [
    "pandas==1.3.5",
    "pandas-profiling==3.1.0",
    "dataclasses-json==0.5.7",
    "great_expectations==0.15.2",
    "jinja2==3.0.3",
    "jsonschema==4.4.0",
    "networkx",
    "ruamel.yaml==0.17.17",
    "numpy==1.21.5",
    "docopt==0.6.2",
    "plotly-express==0.4.1",
    "tqdm==4.64.0",
    "kaleido==0.2.1",
]

if not on_rtd:
    install_requires.append("networkit==9.1.1")

setup_requires = ["pytest-runner"]

tests_require = ["pytest", "pytest-cov"]

extras_require = {
    "tests": tests_require,
    "docs": [
        "sphinx",
        "sphinx-rtd-theme",
        "sphinx-click",
        "sphinx-autodoc-typehints",
        "sphinx_automodapi",
        "nbsphinx_link",
        "jupyter-sphinx",
        "sphinxcontrib-pseudocode",
    ],
}

keywords = [
    "ontology",
    "alignment",
]

setup(
    name="onto_merger",
    packages=find_packages(where="onto_merger"),
    package_dir={"": "onto_merger"},
    version="0.1.0",
    license="Apache License, Version 2.0",
    description="OntoMerger is an ontology alignment library for deduplicating knowledge graph nodes,(i.e. ontology "
    + "concepts) that represent the *same domain*, e.g. diseases, and **connecting** them to form a "
    + "single directed acyclic hierarchical graph (DAG) (i.e. an ontology class hierarchy).",
    author="David Geleta",
    author_email="davidgeleta@gmail.com",
    url="https://github.com/AstraZeneca/onto_merger",
    download_url="https://github.com/AstraZeneca/onto_merger/archive/v0.1.0.tar.gz",
    keywords=keywords,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    extras_require=extras_require,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
