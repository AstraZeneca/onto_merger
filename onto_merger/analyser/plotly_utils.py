from typing import List

import pandas as pd
from pandas import DataFrame
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from onto_merger.analyser.constants import COLUMN_NAMESPACE, COLUMN_NAMESPACE_FREQ, \
    COLUMN_NAMESPACE_SOURCE_ID, COLUMN_NAMESPACE_TARGET_ID, COLUMN_COUNT, COLUMN_FREQ

_FIGURE_FORMAT = "svg"
_COLOR_WHITE = "#fff"
_WIDTH_ONE_COL_ROW = 1100
_WIDTH_TWO_COL_ROW = round(_WIDTH_ONE_COL_ROW / 2)


def _get_figure_filepath(file_path: str) -> str:
    return f"{file_path}.{_FIGURE_FORMAT}"


def _format_percentage_column_to_decimal_places(analysis_table: DataFrame,
                                                column_name: str,
                                                decimal: int = 2) -> DataFrame:
    analysis_table[column_name] = analysis_table.apply(
        lambda x: round(x[column_name], decimal),
        axis=1,
    )
    return analysis_table


def produce_nodes_ns_freq_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    px.bar(
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
        .update_yaxes(autorange="reversed") \
        .write_image(_get_figure_filepath(file_path=file_path))


def produce_node_status_stacked_bar_chart(
        analysis_table: DataFrame,
        file_path: str,
        is_one_bar: bool,
):
    # adjust setting for 1 vs multiple bars
    if is_one_bar is True:
        showaxis = False
        height = 50
        margin_right = 0
    else:
        showaxis = True
        height = 300
        margin_right = 30

    # produce image
    px.bar(
        analysis_table,
        y="category",
        x="ratio",
        text="status",
        color="status_no_freq",
        orientation='h',
        width=_WIDTH_ONE_COL_ROW,
        height=height,
        labels={'ratio': 'Nodes (%)', 'status_no_freq': 'Stage', 'category': ''},
    ) \
        .update_layout({
        "plot_bgcolor": _COLOR_WHITE,
        'showlegend': False,
        'margin': {
            "l": 0,  # left
            "r": margin_right,  # right
            "t": 0,  # top
            "b": 0,  # bottom
        }
    }) \
        .update_yaxes(autorange="reversed") \
        .update_traces(textposition='inside', textfont_size=14, textfont_color="white") \
        .update_xaxes(visible=showaxis) \
        .update_yaxes(visible=showaxis) \
        .write_image(_get_figure_filepath(file_path=file_path))


def produce_mapping_type_freq_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
    px.bar(
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
        .update_yaxes(autorange="reversed") \
        .write_image(_get_figure_filepath(file_path=file_path))


def produce_merged_nss_stacked_bar_chart(
        analysis_table: DataFrame,
        file_path: str,
) -> None:
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
        y: str, x: str, text: str, color: str
) -> None:
    px.bar(
        _format_percentage_column_to_decimal_places(
            analysis_table=analysis_table,
            column_name=COLUMN_FREQ,
        ),
        y=y,
        x=x,
        text=text,
        color=color,
        orientation='h',
        width=_WIDTH_ONE_COL_ROW,
        labels=label_replacement,
    ) \
        .update_layout(
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
    ) \
        .write_image(_get_figure_filepath(file_path=file_path))


def produce_edge_heatmap(analysis_table: DataFrame, file_path: str):
    data = analysis_table.copy()
    data = data[list(data.columns)].replace({'0': np.nan, 0: np.nan})
    px.imshow(
        data,
        text_auto=True,
        color_continuous_scale="blues"
    ) \
        .update_xaxes(side="top") \
        .update_layout(plot_bgcolor=_COLOR_WHITE) \
        .write_image(_get_figure_filepath(file_path=file_path))


def produce_gantt_chart(
        analysis_table: DataFrame,
        file_path: str,
        label_replacement: dict,
) -> None:
    px.timeline(
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
        .update_yaxes(autorange="reversed") \
        .write_image(_get_figure_filepath(file_path=file_path))


def produce_vertical_bar_chart_stacked(
        analysis_table: DataFrame,
        file_path: str,
):
    px.bar(
        analysis_table,
        x="step_name",
        y="freq",
        labels={'status': 'Status', 'freq': 'Ratio of nodes (%)', 'step_name': 'Step'},
        color="status",
        text="freq",
        width=_WIDTH_ONE_COL_ROW,
        height=400,
    )\
        .update_layout(plot_bgcolor=_COLOR_WHITE) \
        .update_xaxes({"tickmode": "linear"}) \
        .write_image(_get_figure_filepath(file_path=file_path))


# todo remove unused
def produce_vertical_bar_chart_node_ns():
    df = pd.DataFrame([
        ["MEDDRA", 123455, 0.80],
        ["MONDO", 22000, 0.19],
        ["EFO", 2342, 0.20],
    ], columns=["namespace", "namespace_count"])
    fig = px.bar(
        df,
        x="namespace_count",
        y="namespace",
        text="namespace_count",
        labels={'namespace': 'Namespace', 'namespace_count': 'Count'}
    )
    fig.update_layout(plot_bgcolor="#fff", showlegend=False).update_yaxes(autorange="reversed")
    fig.write_image("plotly_vertical_bar_node_ns.svg")


def produce_vertical_bar_chart():
    df = pd.DataFrame([
        [1, 0],
        [2, 50],
        [3, 100],
    ], columns=["step", "mapped %"])

    fig = px.bar(df,
                 x="step",
                 y="mapped %",
                 text="mapped %"
                 )
    fig \
        .update_layout(plot_bgcolor="#fff") \
        .update_xaxes({"tickmode": "linear"})
    fig.write_image("plotly_vertical_bar.svg")
