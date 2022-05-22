from typing import List
import itertools

import pandas as pd
from pandas import DataFrame

from onto_merger.analyser.constants import COLUMN_NAMESPACE_TARGET_ID, COLUMN_NAMESPACE_SOURCE_ID
from onto_merger.data.constants import COLUMN_TARGET_ID, COLUMN_SOURCE_ID
from onto_merger.data.dataclasses import NamedTable


# MERGE #
def _produce_merge_cluster_analysis(merges_aggregated: DataFrame) -> List[NamedTable]:
    column_cluster_size = 'cluster_size'
    column_many_to_one_nss = 'many_to_one_nss'
    column_many_to_one_nss_size = f"{column_many_to_one_nss}_size"

    # clusters
    df = merges_aggregated.copy()
    df = df[[COLUMN_TARGET_ID, COLUMN_SOURCE_ID,
             COLUMN_NAMESPACE_TARGET_ID, COLUMN_NAMESPACE_SOURCE_ID]] \
        .groupby([COLUMN_TARGET_ID, COLUMN_NAMESPACE_TARGET_ID]) \
        .agg(cluster_size=(COLUMN_SOURCE_ID, lambda x: len(list(x)) + 1),
             merged_nss_unique_count=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: len(set(x))),
             merged_nss_count=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: len(list(x))),
             merged_nss_unique=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: set(x)),
             merged_nss=(COLUMN_NAMESPACE_SOURCE_ID, lambda x: list(x))) \
        .reset_index() \
        .sort_values(column_cluster_size, ascending=False)

    # describe: cluster_size
    df_cluster_size_describe = df[[column_cluster_size]] \
        .describe() \
        .reset_index(level=0)
    print(df_cluster_size_describe)

    # cluster size | x: size bins | y: freq
    df_bins = df[[column_cluster_size]] \
        .groupby([column_cluster_size]) \
        .agg(size_bin=(column_cluster_size, 'count')) \
        .reset_index()

    # namespaces in clusters
    df[column_many_to_one_nss] = df.apply(lambda x: set([key
                                                         for key, group in itertools.groupby(x['merged_nss'])
                                                         if len(list(group)) > 1]), axis=1)
    df[column_many_to_one_nss_size] = df.apply(lambda x: len(x[column_many_to_one_nss]), axis=1)
    df_many_nss_merged_to_one = df[[COLUMN_TARGET_ID, column_many_to_one_nss, column_many_to_one_nss_size]]
    df_many_nss_merged_to_one = df_many_nss_merged_to_one[df_many_nss_merged_to_one[column_many_to_one_nss_size] > 0]
    members = df_many_nss_merged_to_one[column_many_to_one_nss].tolist()
    flat_list = list(set([item for sublist in members for item in sublist]))
    for ns in flat_list:
        df_many_nss_merged_to_one[ns] = \
            df_many_nss_merged_to_one.apply(lambda x: 1 if ns in x[column_many_to_one_nss] else 0, axis=1)
    df_many_nss_merged_to_one = df_many_nss_merged_to_one.sort_values(column_many_to_one_nss_size, ascending=False)
    nss_data = [
        (ns, df_many_nss_merged_to_one[ns].sum())
        for ns in flat_list
    ]
    df_nss_analysis = pd.DataFrame(nss_data, columns=["namespace", "count_occurs_multiple_times_in_cluster"])\
        .sort_values("count_occurs_multiple_times_in_cluster", ascending=False)

    return [
        NamedTable("clusters", df),
        NamedTable("cluster_size_description", df_cluster_size_describe),
        NamedTable("cluster_size_bin_freq", df_bins),
        NamedTable("many_nss_merged_to_one", df_many_nss_merged_to_one),
        NamedTable("many_nss_merged_to_one_freq", df_nss_analysis),
    ]
