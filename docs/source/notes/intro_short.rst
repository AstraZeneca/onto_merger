OntoMerger is an ontology alignment library for **deduplicating** knowledge
graph nodes,
(i.e. ontology concepts) that represent the *same domain*, e.g. diseases,
and **connecting** them to
form a single directed acyclic hierarchical graph (DAG) (i.e. an ontology
class hierarchy).
The library implements a pipeline that takes *nodes, mappings and
(disconnected) hierarchies* as input and produces *node merges* and a
*connected hierarchy*.
It also provides analysis and data testing for fine tuning the inputs in order
to further reduce duplication, as well as to increase connectivity.

.. code-block:: latex

     >@article{ontomerger,
               arxivId = {???},
               author = {Geleta, David and Rozemberczki, Benedek
                        and Nikolov, Andriy and O'Donoghue, Mark
                        and Gogleva, Anna and Tamma, Valentina},
               month = {may},
               title = {{OntoMerger: An Ontology Alignment Library for
                         Creating Minimal and Connected Domain Knowledge
                         Sub-graphs.}},
               url = {http://arxiv.org/abs/???},
               year = {2022}
     }
