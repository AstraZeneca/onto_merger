"""Alignment process runner and helper methods."""

from typing import List, Tuple

import pandas as pd
from pandas import DataFrame

from onto_merger.alignment import mapping_utils, merge_utils
from onto_merger.analyser import analysis_utils
from onto_merger.data.constants import (
    COLUMN_MAPPING_TYPE_GROUP,
    COLUMN_NAMESPACE,
    COLUMN_PROVENANCE,
    COLUMN_RELATION,
    COLUMN_SOURCE_ID_ALIGNED_TO,
    COLUMN_STEP_COUNTER,
    MAPPING_TYPE_GROUP_EQV,
    MAPPING_TYPE_GROUP_XREF,
    ONTO_MERGER,
    RELATION_MERGE,
    SCHEMA_HIERARCHY_EDGE_TABLE,
    SCHEMA_MERGE_TABLE_WITH_META_DATA,
    TABLE_MAPPINGS,
    TABLE_MAPPINGS_FOR_INPUT_NODES,
    TABLE_MAPPINGS_OBSOLETE_TO_CURRENT,
    TABLE_MAPPINGS_UPDATED,
    TABLE_MERGES_WITH_META_DATA,
    TABLE_NODES,
    TABLE_NODES_OBSOLETE,
)
from onto_merger.data.data_manager import DataManager
from onto_merger.data.dataclasses import (
    AlignmentConfig,
    AlignmentStep,
    DataRepository,
    NamedTable,
    convert_alignment_steps_to_named_table,
)
from onto_merger.logger.log import get_logger

logger = get_logger(__name__)


class AlignmentManager:
    """Alignment process pipeline."""

    def __init__(
            self,
            alignment_config: AlignmentConfig,
            data_repo: DataRepository,
            data_manager: DataManager,
    ):
        """Initialise the AlignmentManager class.

        :param alignment_config: The alignment process configuration dataclass.
        :param data_repo: The data repository that stores the input tables.
        :param data_manager: The data manager instance.
        """
        self._alignment_config = alignment_config
        self._data_manager = data_manager
        self._data_repo_input = data_repo

        # store alignment steps data
        self._alignment_steps: List[AlignmentStep] = []

        # store produced data
        self._data_repo_output = DataRepository()
        self._data_repo_output.update(table=DataManager.produce_empty_merge_table())

    def align_nodes(self) -> Tuple[DataRepository, List[str]]:
        """Run the alignment pipeline.

        Results are stored in the internal data repository.

        :return: The output tables in a data repository dataclass, and the source
        alignment order list.
        """
        # prepare for alignment
        self._preprocess_mappings()
        source_alignment_order = _produce_source_alignment_priority_order(
            seed_ontology_name=self._alignment_config.base_config.seed_ontology_name,
            nodes=self._data_repo_input.get(TABLE_NODES).dataframe,
        )

        # (1) use the strongest relations: equivalence
        self._create_initial_step(mapping_type_group_name=MAPPING_TYPE_GROUP_EQV)
        self._align_sources(
            sources_to_align=source_alignment_order,
            mapping_type_group_name=MAPPING_TYPE_GROUP_EQV,
            mapping_types=self._alignment_config.mapping_type_groups.equivalence,
            start_step=0,
        )

        # (2) use the weaker relations: database reference
        self._align_sources(
            sources_to_align=source_alignment_order,
            mapping_type_group_name=MAPPING_TYPE_GROUP_XREF,
            mapping_types=self._alignment_config.mapping_type_groups.database_reference,
            start_step=len(source_alignment_order)
        )

        # remove self merges
        all_merges = self._data_repo_output.get(TABLE_MERGES_WITH_META_DATA).dataframe
        filtered_merges = pd.concat([
            all_merges,
            self._data_repo_output.get("SEED_MERGES").dataframe]).drop_duplicates(keep=False)
        self._data_repo_output.update(
            table=NamedTable(TABLE_MERGES_WITH_META_DATA, filtered_merges)
        )
        logger.info(f"Filtered out seed self merges ({len(all_merges):,d} -> {len(filtered_merges):,d})")

        # save meta data
        self._data_repo_output.update(
            table=convert_alignment_steps_to_named_table(alignment_steps=self._alignment_steps)
        )

        return self._data_repo_output, source_alignment_order

    def _align_sources(
            self,
            sources_to_align: List[str],
            mapping_type_group_name: str,
            mapping_types: List[str],
            start_step: int,
    ) -> None:
        """Run the alignment for each source according to the priority order, for a given mapping type group.

        :param sources_to_align: The source alignment priority order list.
        :param mapping_type_group_name: The name of mapping type group.
        :param mapping_types: The mapping types in the given type group.

        Results are stored in the internal data repository.

        :return:
        """
        for source_id in sources_to_align:
            step_counter = start_step + sources_to_align.index(source_id) + 1
            logger.info(
                f"* * * * * SOURCE: {source_id} | STEP: {step_counter} of "
                + f"{len(sources_to_align) * 2} | MAPPING: {mapping_type_group_name} "
                + "* * * * *"
            )
            (merges_for_source, alignment_step) = self._align_nodes_to_source(
                source_id=source_id,
                step_counter=step_counter,
                mapping_type_group_name=mapping_type_group_name,
                mapping_types=mapping_types,
            )
            self._store_results_from_alignment_step(merges_for_source=merges_for_source, alignment_step=alignment_step)

    def _align_nodes_to_source(
            self,
            source_id: str,
            step_counter: int,
            mapping_type_group_name: str,
            mapping_types: List[str],
    ) -> Tuple[NamedTable, AlignmentStep]:
        """Perform an alignment step to a source.

        :param source_id: The source the unmapped nodes are aligned to.
        :param step_counter: The step number.
        :param mapping_type_group_name: The name of mapping type group.
        :param mapping_types: The mapping types in the given type group.
        :return: The merge named table for the step, and the step meta data dataclass.
        """
        unmapped_nodes = merge_utils.produce_table_unmapped_nodes(
            nodes=self._data_repo_input.get(TABLE_NODES).dataframe,
            merges=self._data_repo_output.get(TABLE_MERGES_WITH_META_DATA).dataframe,
        )

        # (1) get mappings for NS
        mappings_for_ns = mapping_utils.get_mappings_for_namespace(
            namespace=source_id,
            edges=self._data_repo_output.get(TABLE_MAPPINGS_FOR_INPUT_NODES).dataframe,
        )
        alignment_step = AlignmentStep(
            mapping_type_group=mapping_type_group_name,
            source=source_id,
            step_counter=step_counter,
            count_unmapped_nodes=len(unmapped_nodes),
        )

        # (2) filter for permitted mapping types
        mappings_for_permitted_type = mapping_utils.get_mappings_with_mapping_relations(
            permitted_mapping_relations=mapping_types, mappings=mappings_for_ns
        )

        # (3) orient mappings towards NS
        mapping_towards_ns = mapping_utils.orient_mappings_to_namespace(
            required_target_id_namespace=source_id,
            mappings=mappings_for_permitted_type,
        )

        # (4) get 1..n : 1 mappings for unmapped nodes
        mappings_deduplicated = mapping_utils.deduplicate_mappings_for_type_group(
            mapping_type_group_name=mapping_type_group_name, mappings=mapping_towards_ns
        )
        mappings_for_unmapped_nodes = mapping_utils.filter_mappings_for_node_set(
            nodes=unmapped_nodes,
            mappings=mappings_deduplicated,
        )
        mappings_one_or_many_source_to_one_target = mapping_utils.get_one_or_many_source_to_one_target_mappings(
            mappings=mappings_for_unmapped_nodes,
        )
        mappings_one_source_to_many_target_mappings = mapping_utils.get_one_source_to_many_target_mappings(
            mappings=mappings_for_unmapped_nodes,
        )
        self._data_manager.save_dropped_mappings_table(
            table=mappings_one_source_to_many_target_mappings,
            step_count=step_counter,
            source_id=source_id,
            mapping_type=mapping_type_group_name,
        )
        alignment_step.count_mappings = len(mappings_for_unmapped_nodes)
        alignment_step.count_nodes_one_source_to_many_target = len(mappings_one_source_to_many_target_mappings)

        # (5) produce return tables
        merge_table = merge_utils.produce_named_table_merges_with_alignment_meta_data(
            merges=mappings_one_or_many_source_to_one_target,
            source_id=source_id,
            step_counter=step_counter,
            mapping_type=mapping_type_group_name,
        )
        alignment_step.count_merged_nodes = len(merge_table.dataframe)
        logger.info(f"Finished aligning nodes onto {source_id}, mapped " + f"{len(merge_table.dataframe):,d} nodes.")

        return merge_table, alignment_step

    def _preprocess_mappings(self) -> None:
        """Preprocess the mappings: internal code reassignments are computed and used to update the full mapping set.

        Results are stored in the internal data repository.

        :return:
        """
        logger.info("Starting to preprocess mappings...")

        # get internal code re-assignment mappings that are 1:1
        # and have the correct mapping relation (e.g. xref could be too weak to merge)
        mappings_obsolete_to_current_node_id = mapping_utils.get_mappings_obsolete_to_current_node_id(
            nodes_obsolete=self._data_repo_input.get(TABLE_NODES_OBSOLETE).dataframe,
            mappings=self._data_repo_input.get(TABLE_MAPPINGS).dataframe,
        )
        mappings_obsolete_to_current_node_id_merge_strength = mapping_utils.get_mappings_with_mapping_relations(
            permitted_mapping_relations=self._alignment_config.mapping_type_groups.equivalence,
            mappings=mappings_obsolete_to_current_node_id,
        )

        # get the mappings without the internal code reassignment and update
        # any obsolete node IDs
        mappings_updated = mapping_utils.get_mappings_with_updated_node_ids(
            mappings=self._data_repo_input.get(TABLE_MAPPINGS).dataframe,
            mappings_obsolete_to_current_node_id=mappings_obsolete_to_current_node_id_merge_strength,
        )
        self._data_repo_output.update(table=NamedTable(name=TABLE_MAPPINGS_UPDATED, dataframe=mappings_updated))

        # mappings that cover input nodes
        mappings_for_input_nodes = mapping_utils.filter_mappings_for_input_node_set(
            input_nodes=self._data_repo_input.get(TABLE_NODES).dataframe,
            mappings=mappings_updated,
        )
        self._data_repo_output.update(
            table=NamedTable(name=TABLE_MAPPINGS_FOR_INPUT_NODES, dataframe=mappings_for_input_nodes)
        )

        #
        mappings_obsolete_to_current_node_id_applicable = mapping_utils.get_nodes_with_updated_node_ids(
            nodes=self._data_repo_input.get(TABLE_NODES).dataframe,
            mappings_obsolete_to_current_node_id=mappings_obsolete_to_current_node_id_merge_strength,
        )
        mappings_obsolete_to_current_node_id_applicable[COLUMN_RELATION] = RELATION_MERGE
        mappings_obsolete_to_current_node_id_applicable[COLUMN_PROVENANCE] = ONTO_MERGER
        self._data_repo_output.update(
            table=NamedTable(
                name=TABLE_MAPPINGS_OBSOLETE_TO_CURRENT,
                dataframe=mappings_obsolete_to_current_node_id_applicable[SCHEMA_HIERARCHY_EDGE_TABLE],
            )
        )

        mappings_obsolete_to_current_node_id_applicable[COLUMN_STEP_COUNTER] = 0
        mappings_obsolete_to_current_node_id_applicable[COLUMN_SOURCE_ID_ALIGNED_TO] = "INTERNAL"
        mappings_obsolete_to_current_node_id_applicable[COLUMN_MAPPING_TYPE_GROUP] = MAPPING_TYPE_GROUP_EQV
        self._data_repo_output.update(
            table=DataManager.merge_tables_of_same_type(
                tables=[NamedTable(TABLE_MERGES_WITH_META_DATA,
                                   mappings_obsolete_to_current_node_id_applicable[SCHEMA_MERGE_TABLE_WITH_META_DATA]),
                        self._data_repo_output.get(TABLE_MERGES_WITH_META_DATA)]
            )
        )

        logger.info("Finished pre-processing mappings.")

    def _create_initial_step(self, mapping_type_group_name: str) -> None:
        """Produce and store the initial set of merges (self merges for the seed ontology) and the step meta data.

        Results are stored in the internal data repository.

        :param mapping_type_group_name: The name of mapping type group used for the
        first batch of alignment steps.
        :return:
        """
        # produce initial merges (seed nodes are merged to themselves,
        # hence they appear mapped)
        self_merges_for_seed_nodes = mapping_utils.produce_self_merges_for_seed_nodes(
            seed_id=self._alignment_config.base_config.seed_ontology_name,
            nodes=self._data_repo_input.get(TABLE_NODES).dataframe,
            nodes_obsolete=self._data_repo_input.get(TABLE_NODES_OBSOLETE).dataframe,
        )
        self._data_repo_output.update(table=self_merges_for_seed_nodes)
        self._data_repo_output.update(table=NamedTable("SEED_MERGES", self_merges_for_seed_nodes.dataframe))

        # record start step meta data
        step = AlignmentStep(
            mapping_type_group=mapping_type_group_name,
            source="INITIALISATION",
            step_counter=0,
            count_unmapped_nodes=(len(self._data_repo_input.get(TABLE_NODES).dataframe)),
        )
        step.count_mappings = len(self_merges_for_seed_nodes.dataframe)
        step.count_merged_nodes = step.count_mappings
        step.task_finished()
        self._alignment_steps.append(
            step
        )

    def _store_results_from_alignment_step(self, merges_for_source: NamedTable, alignment_step: AlignmentStep) -> None:
        """Store the results of an aligment step (merges and step meta data) in the internal data repository.

        :param merges_for_source: The merges produced during the alignemt step.
        :param alignment_step: The alignment step meta data.
        :return:
        """
        alignment_step.task_finished()
        self._alignment_steps.append(alignment_step)
        self._data_repo_output.update(
            table=DataManager.merge_tables_of_same_type(
                tables=[merges_for_source, self._data_repo_output.get(TABLE_MERGES_WITH_META_DATA)]
            )
        )


def _produce_source_alignment_priority_order(seed_ontology_name: str, nodes: DataFrame) -> List[str]:
    """Produce the alignment process source priority order.

    The alignment order is produced by putting the seed ontology as first (this
    should have the most mappings and the desired hierarchy), and the rest of the
    ontologies according to the frequency of the nodes.

    :param seed_ontology_name: The name of the seed ontology.
    :param nodes: The node table used to compute the node source frequency.
    :return: The list of ontology names.
    """
    priority_order = [seed_ontology_name]
    ontology_namespaces = list(
        analysis_utils.produce_table_node_namespace_distribution(node_table=nodes)[COLUMN_NAMESPACE]
    )
    ontology_namespaces.remove(seed_ontology_name)
    priority_order.extend(ontology_namespaces)
    return priority_order
