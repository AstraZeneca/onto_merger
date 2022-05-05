# Development plan

## Project
- [X] project setup
- [X] logger

## Test Data
- [X] test data (download as CSV from Databricks)
    - [X] nodes to merge 
    - [X] list of deprecated nodes 
    - [X] supporting mappings
    - [X] supporting hierarchies
- [X] MONDO proper hierarchy: one connected DAG
- [ ] parse ont-s
   - [ ] MONDO with subsumption mappings, excluding non Disease nodes
   - [ ] others for background knowledge

## Modules 

### Pipeline

LOADING
    
ALIGNMENT >> Mapping manager 

CONNECTIVITY >> Hierarchy manager


MERGING
- [ ] produce output: apply mappings
    - [X] merge table 
    - [X] nodes merged (id, merged_onto)
    - [X] nodes unmerged
           
EVALUATE
- [X] Profiler
- [X] Merged ont analyser


### Config

- [X] JSON schema
- [X] validate schema
- [X] config to dataclass
- [X] validate data WRT input >> GE
- [ ] error report

### Data manager

- [x] load
    - [X] load nodes to reconcile 
    - [X] load nodes obsolete
    - [x] load mappings 
    - [x] load hierarchies 
    - [x] load config
    

### Alignment

- [ ] save dropped mappings for NS (many to ones) for debug
- [ ] step result table


### Mapping manager 

- [ ] remap mapping set get_mappings_with_updated_node_ids
- [X] check multiplicity 


### Hierarchy manager

- [X] add seed hierarchy
    - [X] remove nodes that are not Disease
- [ ] add other required hierarchies: break up
- [ ] connect unmapped nodes


### Merged ont analyser
- [ ] sections
    - [ ] summary
    - [X] nodes
    - [ ] connectivity
    - [ ] merges (x1)
    - [ ] mappings (x3)
- [X] HTML report
- [X] links to Profiler reports


### Data tester

- [X] GE FW
    - [X] jinja2 import clash !!!
    - [X] helper
        - [ ] mapping relations
    - [X] utils
        - [X] path to data docs
    - [X] runner 
- [X] input tables
- [ ] output tables
    - [ ] merges
    - [X] edges hierarchy
    - [ ] nodes unmapped

## Refactoring

- [X] DATA REPO 
    - [X] update in modules
    - [X] merged nodes
- [X] return data as LoadedTable to pipeline
- [X] pipeline.py some methods move to MergeManager
- [X] unmerged nodes
- [ ] column names as constants
- [ ] filtering dataframe for node IDs make more efficient (several places)

## DOCUMENTATION

- [ ] comment all modules
- [ ] Sphinx doc 
- [ ] RTD config

## TESTING

- [X] pytest setup
- [X] tox setup
    - [ ] fix requirements
    - [ ] extended setup
- [ ] test all modules
    - [ ] alignment x4
    - [ ] alg config x1
    - [ ] analyser x3
    - [ ] data x3
    - [ ] data_testing x3
    - [ ] logger
    - [ ] pipeline x1
    

# Bugfixes

- [ ] analyser/analysis_util.py:20: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.

-----

# Notes 

Processing with Pandas first then maybe Spark ?