.. _Alignment configuration:

Alignment configuration
=========================

The *alignment configuration JSON* is a required input that must be placed in
the ``PROJECT_FOLDER/input`` folder and named as ``config.json``.


.. note::
    The JSON schema and content is validated prior to running the alignment
    process.



Specification
--------------

The JSON must contain all of the following properties:

* ``domain_node_type``: the type of the domain nodes, e.g. *Disease*.
* | ``seed_ontology_name``: the *seed* ontology name (i.e. the namespace part
  | of the corresponding node IDs.).
* | ``required_full_hierarchies``: the list of ontologies that the API will try
  | to integrate as much as possible. This must have at least one item:
  | the seed ontology name.
* ``mappings``: mapping grouping and directionality specification.

    * | ``type_groups``: groups the available mapping relations into three
      | *strength categories* that are handled differently during the alignment
      | process; all three type groups must be present.

        * | ``equivalence``: the strongest mapping relation category; must
          | contain at least one mapping relation type.
        * | ``database_reference``: the second strongest mapping relation
          | category that can be an empty list.
        * | ``label_match``: the weakest mapping relation category that can be
          | an empty list.

    * | ``directionality``: groups the available mapping relations into two
      | *direction categories* that  are handled differently during the
      | alignment process; all two type groups must be present.

        * ``uni``: can be an empty list.
        * | ``symmetric``: must contain at least one mapping relation type
          | (equivalence).



Example
-----------

.. literalinclude:: ../_static/config.json
   :language: json
   :caption: The alignment configuration JSON.
