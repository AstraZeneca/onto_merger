# Notes on data used in this repository

The repository contains two data sets:

1. ```data/bikg_disease```: example data set
2. ```tests/test_data```: unit and integration test data set

**All** of these data sets only use publicly available data such that the licensing permits
usage in this context (for details please see the Section *(1) Example data set*).

The data sets are located in the following paths:

```
onto_merger
├── ...
├── data
│   └── bikg_disease
│       ├── config.json
│       ├── edges_hierarchy.csv
│       ├── mappings.csv
│       ├── nodes.csv
│       └── nodes_obsolete.csv
└── tests
    ├── test_data_invalid
    |   └── config.json
    └── test_data
        ├── config.json
        ├── edges_hierarchy.csv
        ├── mappings.csv
        ├── nodes.csv
        └── nodes_obsolete.csv
```

## (1) Example data set


The data set contains (fragments) of the following ontologies:

| Ontology ID | Link | License | Source | 
|:--|:--|:--|:--|
| DOID | [Link](https://www.ebi.ac.uk/ols/ontologies/doid) | CC0 | EBI | 
| EFO | [Link](https://www.ebi.ac.uk/ols/ontologies/efo) | Apache LICENSE-2.0 |EBI |
| ICD10CM | [Link](https://www.cms.gov/medicare/icd-10/2021-icd-10-cm) | ??? | UMLS |
| MEDDRA | [Link](https://www.meddra.org/faq) | ??? | ??? |
| MESH | [Link](https://www.nlm.nih.gov/databases/download/mesh.html) | [Link]() | ??? | UMLS |
| MONDO | [Link](https://www.ebi.ac.uk/ols/ontologies/mondo) | CC4 | EBI |
| ORPHANET | [Link](https://www.orpha.net/consor/cgi-bin/Education_AboutOrphanet.php?lng=EN&stapage=CGUp) | ??? | ??? |
| SNOMED | [Link](https://www.nlm.nih.gov/databases/download/mesh.html) | ??? | UMLS |


## (2) Test data sets

There are two test data sets stored in the aforementioned locations. In addition we use small data fragments 
in unit tests. 

### (2.1) ```test_data_invalid``` data set

This is not a proper data set. The folder only contains a wrongly configured JSON that will cause the 
pipeline to fail.

### (2.2) ```test_data``` data set

This is a proper subset of the ```bikg_disease``` data set. There are no additional data points.
This data set is smaller than ```bikg_disease``` to reduce runtime. 
It is used for integration test, where the full pipeline is run end to end.

### (2.3) Unit test data 

Throughout the unit tests we use a mixture of real and made up node identifiers, mappings and hierarchy edges.
All of the real data  only use publicly available data such that the licensing permits usage in this context.