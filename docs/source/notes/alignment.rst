.. _Alignment:

Alignment
=========

The alignment process takes available mappings and produces a set of stable
**merges** (``output/domain_ontology/merges.csv``).
In :ref:`target to merge example 1` the node ``SNOMED:31387002`` is deemed
the same, i.e. a duplicate, of thenode ``MONDO:0004979`` (indeed both nodes
represent the disease concept
`asthma <https://www.ebi.ac.uk/ols/ontologies/mondo/terms?iri=http%3A%2F%2Fpurl.obolibrary.org%2Fobo%2FMONDO_0004979>`_).

.. _target to merge example 1:

.. table:: merge example (one to one)

    +-----------------+---------------+
    | source_id       | target_id     |
    +-----------------+---------------+
    | SNOMED:31387002 | MONDO:0004979 |
    +-----------------+---------------+


*Stable* means that from the perspective of the *source* ontology there are
no *splits*. In :ref:`target to merge example 2` two ``OMIM`` nodes are mapped
to the same ``MONDO`` nodes, where we retain the concept representation
granularity of the target ontology but loose it from the perspective of the
source ontology.


.. _target to merge example 2:

.. table:: merge example (many to one)

    +-----------------+---------------+
    | source_id       | target_id     |
    +-----------------+---------------+
    | OMIM:608584     | MONDO:0004979 |
    +-----------------+---------------+
    | OMIM:600807     | MONDO:0004979 |
    +-----------------+---------------+

In cases where a source concept would match to two different concepts of the
target ontology, merging is inappropriate and it is handled in the
connectivity process.



Steps
-----------

Establishing source alignment order
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First we produce the source alignment order. The alignment process is a
sequence of steps, where in each
step we attempt to merge nodes to a given ontology.
The goal is to have the minimal number of nodes, from the minimal number of
different sources.
Therefore the alignment order is produced by putting the *seed* ontology as
first (this should have the most mappings and the desired hierarchy), and the
rest of the ontologies according to the frequency of the
nodes. For example, in the
:ref:`example data set <target to node source frequency chart>`
this would be ``MONDO, MEDDRA, ICD10CM, MESH, ...``.



Pre-processing mappings
^^^^^^^^^^^^^^^^^^^^^^^^

Next we preprocess the mappings.

Mappings typically contain *internal code reassignments*.
These are mappings between nodes of the same source that
describe the new code(s) of deprecated nodes. In the
:ref:`target to internal code reassignment` the
node ``MONDO:0022856`` was
`deprecated <https://www.ebi.ac.uk/ols/ontologies/mondo/terms?iri=http%3A%2F%2Fpurl.obolibrary.org%2Fobo%2FMONDO_0022856>`_
and
`replaced <https://www.ebi.ac.uk/ols/ontologies/mondo/terms?iri=http%3A%2F%2Fpurl.obolibrary.org%2Fobo%2FMONDO_0001217>`_
by the node ``MONDO:0001217``.  The input table ``nodes_obsolete.csv``
helps to determine the deprecated and the current node ID (some input mappings
may be parsed inconsistently regarding to directionality).


.. _target to internal code reassignment:

.. table:: internal code reassignment mapping example

    +-----------------+---------------+
    | source_id       | target_id     |
    +-----------------+---------------+
    | MONDO:0022856   | MONDO:0001217 |
    +-----------------+---------------+


These are removed from the full mapping sets (``mappings.csv``).
The remainder mapping set is updated using
the *internal code reassignments* mappings (the full mapping set may contain
mappings from different sources,
that are not necessarily up to date, as described
:ref:`here <target to version discrepancies>`).



Aligning sources for mapping type groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The alignment is run in several batches, where each batch aligns nodes
to each source as specified by the *source alignment order*. First it uses the
strongest mapping relation type group, *equivalence*,
then *database reference* and the rest.


Aligning nodes to a source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This process is repeated for each source.

#. | **Filter mappings for the source**: filters the available mappings to
   | find all where either the source or the target node is from the given
   | source (e.g. ``MONDO``)
#. | **Filter mappings for the permitted type**: filters source mappings for
   | the mapping type group (e.g. ``equivalence``)
#. | **Orient mappings towards source**: source and target node IDs are
   | potentially flipped so the target node ID is always from the ontology
   | that we are aligning onto (e.g. ``MONDO:0004979, OMIM:608584`` becomes
   | ``MONDO:0004979, OMIM:608584``)
#. | **Get one or many source to one target node mappings**:

    * | **mappings are de-duplicated**: if we have two mappings where the
      | source and target node IDs are the same, but the mapping relation is
      | different, the two mappings are reduced to one (note that as these are
      | in the same type group)
    * | **filtering for unmapped nodes**: only those mappings are retained
      | where the source node ID is unmapped.
    * | **filtering for multiplicity**: only those mappings are kept that wont
      | form a one source to many target (i.e. split) mapping cluster, i.e.
      | in the remaining mappings are one or many source to one target node
      | mappings (the rest of such mappings are dropped, these are saved
      | for debugging in the folder:
      | ``PROJECT_FOLDER/output/intermediate/dropped_mappings`` folder, with
      | step ID, aligned source ID and the mapping strength e.g.
      | ``../equivalence_1_MONDO.csv``)

#. **Filtered mappings are saved as merges to the source**

