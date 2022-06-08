"""Helpers for producing analysis plots."""

from typing import Union

import numpy as np
import plotly.express as px
from pandas import DataFrame

from onto_merger.analyser.constants import (
    COLUMN_FREQ,
    COLUMN_NAMESPACE,
    COLUMN_NAMESPACE_FREQ,
    COLUMN_NAMESPACE_SOURCE_ID,
    COLUMN_NAMESPACE_TARGET_ID,
)

_COLOR_WHITE = "#fff"
_WIDTH_ONE_COL_ROW = 1100
_WIDTH_TWO_COL_ROW = round(_WIDTH_ONE_COL_ROW / 2)


def produce_nodes_ns_freq_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce a node namespace frequency chart figure .

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    if file_path.endswith(".html"):
        return

    fig = px.bar(
        _format_percentage_column_to_decimal_places(
            analysis_table=analysis_table,
            column_name=COLUMN_NAMESPACE_FREQ,
        ),
        x=COLUMN_NAMESPACE_FREQ,
        y=COLUMN_NAMESPACE,
        text=COLUMN_NAMESPACE_FREQ,
        orientation='h',
        labels={COLUMN_NAMESPACE: 'Node Origin', COLUMN_NAMESPACE_FREQ: 'Frequency (%)'}
    ) \
        .update_layout(plot_bgcolor=_COLOR_WHITE) \
        .update_yaxes(autorange="reversed")

    fig.write_image(file_path)


def produce_status_stacked_bar_chart(
        analysis_table: DataFrame,
        file_path: str,
        is_one_bar: bool,
        showlegend: bool = False,
        labels: Union[dict, None] = None,
) -> None:
    """Produce a node status bar chart figure .

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :param is_one_bar: If True the figure height will be calculated for one bar, otherwise for multiple bars.
    :param showlegend: If True the legend will be display, otherwise hidden.
    :param labels: If given it will rename the axis & legend labels.
    :return:
    """
    if file_path.endswith(".html"):
        return

    # adjust setting for 1 vs multiple bars
    if is_one_bar is True:
        showaxis = False
        height = 100
    else:
        showaxis = True
        height = 300

    # produce image
    fig = px.bar(
        analysis_table,
        y="category",
        x="ratio" if "ratio" in list(analysis_table) else "count",
        text="status" if "status" in list(analysis_table) else "status_no_freq",
        color="status_no_freq" if "status_no_freq" in list(analysis_table) else None,
        orientation='h',
        width=_WIDTH_ONE_COL_ROW,
        height=height,
        labels=(
            {'ratio': 'Nodes (%)', 'status_no_freq': 'Stage', 'category': ''}
            if labels is None else labels
        ),
    ) \
        .update_layout(
        {
            "plot_bgcolor": _COLOR_WHITE,
            'showlegend': showlegend,
            'margin': {
                "l": 0,  # left
                "r": 0,  # right
                "t": 0,  # top
                "b": 0,  # bottom
            }
        }
    ) \
        .update_yaxes(autorange="reversed") \
        .update_traces(textposition='inside', textfont_size=14, textfont_color="white") \
        .update_xaxes(visible=showaxis) \
        .update_yaxes(visible=showaxis)

    fig.write_image(file_path)


def produce_status_stacked_bar_chart_edge(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce an edge hierarchy bar chart figure .

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    if file_path.endswith(".html"):
        return

    # produce image
    fig = px.bar(
        analysis_table,
        y="category",
        x="freq",
        text="status_no_freq",
        color="status",
        orientation='h',
        width=_WIDTH_ONE_COL_ROW,
        height=85,
        labels=(
            {'status': 'Status', 'status_no_freq': 'Stage', 'category': ''}
        ),
    ) \
        .update_layout(
        {
            "plot_bgcolor": _COLOR_WHITE,
            'showlegend': True,
            'margin': {
                "l": 0,  # left
                "r": 0,  # right
                "t": 0,  # top
                "b": 0,  # bottom
            }
        }
    ) \
        .update_traces(textposition='inside', textfont_size=14, textfont_color="white") \
        .update_xaxes(visible=False) \
        .update_yaxes(visible=False)

    fig.write_image(file_path)


def produce_mapping_type_freq_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce a mapping type frequency figure .

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    if file_path.endswith(".html"):
        return

    fig = px.bar(
        _format_percentage_column_to_decimal_places(
            analysis_table=analysis_table,
            column_name=COLUMN_FREQ,
        ),
        x=COLUMN_FREQ,
        y="relation",
        text=COLUMN_FREQ,
        orientation='h',
        labels={'relation': '', COLUMN_FREQ: 'Frequency (%)'}
    ) \
        .update_layout(plot_bgcolor=_COLOR_WHITE) \
        .update_yaxes(autorange="reversed")

    fig.write_image(file_path)


def produce_merged_nss_stacked_bar_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce a bar chart figure for merged namespaces.

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    _produce_directed_edge_stacked_bar_chart(
        analysis_table=analysis_table,
        file_path=file_path,
        label_replacement={
            COLUMN_NAMESPACE_TARGET_ID: 'Canonical',
            COLUMN_NAMESPACE_SOURCE_ID: 'Merged',
            COLUMN_FREQ: 'Frequency (%)'
        },
        y=COLUMN_NAMESPACE_TARGET_ID,
        x=COLUMN_FREQ,
        text=COLUMN_NAMESPACE_SOURCE_ID,
        color=COLUMN_NAMESPACE_SOURCE_ID,
    )


def produce_hierarchy_nss_stacked_bar_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce a bar chart figure for mapped namespaces.

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    _produce_directed_edge_stacked_bar_chart(
        analysis_table=analysis_table,
        file_path=file_path,
        label_replacement={
            COLUMN_NAMESPACE_TARGET_ID: 'Parent',
            COLUMN_NAMESPACE_SOURCE_ID: 'Child',
            COLUMN_FREQ: 'Child Frequency (%)'
        },
        y=COLUMN_NAMESPACE_TARGET_ID,
        x=COLUMN_FREQ,
        text=COLUMN_NAMESPACE_SOURCE_ID,
        color=COLUMN_NAMESPACE_SOURCE_ID,
    )


def _produce_directed_edge_stacked_bar_chart(
        analysis_table: DataFrame,
        file_path: str,
        label_replacement: dict,
        y: str, x: str, text: str, color: str,
) -> None:
    if file_path.endswith(".html"):
        return

    data = _format_percentage_column_to_decimal_places(
        analysis_table=analysis_table,
        column_name=COLUMN_FREQ,
    )
    fig = px.bar(
        data,
        y=y,
        x=x,
        height=_compute_dynamic_height(y_axis_column_name=COLUMN_NAMESPACE_TARGET_ID, table=analysis_table),
        text=text,
        color=color,
        orientation='h',
        width=_WIDTH_ONE_COL_ROW,
        labels=label_replacement,
    )
    fig.update_layout(
        {
            "plot_bgcolor": _COLOR_WHITE,
            'margin': {
                "l": 0,  # left
                "r": 0,  # right
                "t": 0,  # top
                "b": 0,  # bottom
            },
            'yaxis': {
                'autorange': 'reversed'
            }
        }
    ) \
        .update_traces(
        textposition='inside',
        textfont_size=14,
        textfont_color="white"
    )

    fig.write_image(file_path)


def produce_edge_heatmap(
        analysis_table: DataFrame, file_path: str
) -> None:
    """Produce a heatmap for mapped namespaces figure.

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    if file_path.endswith(".html"):
        return

    data = analysis_table.copy()
    data = data[list(data.columns)].replace({'0': np.nan, 0: np.nan})
    fig = px.imshow(
        data,
        text_auto=True,
        color_continuous_scale="blues"
    ) \
        .update_xaxes(side="top") \
        .update_layout(plot_bgcolor=_COLOR_WHITE)

    fig.write_image(file_path)


def produce_gantt_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce a Gantt chart figure.

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    if file_path.endswith(".html"):
        return

    fig = px.timeline(
        analysis_table,
        x_start="start",
        x_end="end",
        y="task",
        text="elapsed_sec",
        width=_WIDTH_ONE_COL_ROW,
        labels={
            'task': ''
        },
    ) \
        .update_layout(
        {
            "plot_bgcolor": _COLOR_WHITE,
            'showlegend': False,
            'margin': {
                "l": 0,  # left
                "r": 5,  # right
                "t": 5,  # top
                "b": 25,  # bottom
            }
        }
    ) \
        .update_yaxes(autorange="reversed")

    fig.write_image(file_path)


def produce_vertical_bar_chart_stacked(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce a vertical bar chart figure.

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    if file_path.endswith(".html"):
        return

    fig = px.bar(
        analysis_table,
        x="step_name",
        y="freq",
        labels={'status': 'Status', 'freq': 'Ratio of nodes (%)', 'step_name': 'Step'},
        color="status",
        text="freq",
        width=_WIDTH_ONE_COL_ROW,
        height=400,
    ) \
        .update_layout(plot_bgcolor=_COLOR_WHITE) \
        .update_xaxes({"tickmode": "linear"})

    fig.write_image(file_path)


def produce_vertical_bar_chart_cluster_size_bins(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    """Produce cluster size bins figure.

    :param analysis_table: The data used to produce the figure.
    :param file_path: The path to save the figure.
    :return:
    """
    if file_path.endswith(".html"):
        return

    fig = px.bar(
        analysis_table,
        x="cluster_size",
        y="count",
        labels={'cluster_size': 'Merge cluster size', 'count': 'Cluster count'},
        text="count",
        width=_WIDTH_ONE_COL_ROW,
        height=300,
    ) \
        .update_layout(plot_bgcolor=_COLOR_WHITE)

    fig.write_image(file_path)


# HELPERS #
def _compute_dynamic_height(y_axis_column_name: str, table: DataFrame) -> int:
    offset = 150
    return offset + (len(set(table[y_axis_column_name].tolist())) * 50)


def _format_percentage_column_to_decimal_places(
        analysis_table: DataFrame, column_name: str, decimal: int = 2
) -> DataFrame:
    analysis_table[column_name] = analysis_table.apply(
        lambda x: round(x[column_name], decimal),
        axis=1,
    )
    return analysis_table
