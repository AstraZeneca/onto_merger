from typing import List
import os

import pandas as pd
from pandas import DataFrame
import plotly.express as px
import plotly.graph_objects as go
import numpy as np


def produce_gantt_chart(file_path: str, data: List[dict]):
    df = pd.DataFrame([
        dict(Task="Job A", Start='2009-01-01', Finish='2009-02-28', Duration='10s'),
        dict(Task="Job B", Start='2009-03-05', Finish='2009-04-15', Duration='10s'),
        dict(Task="Job C", Start='2009-02-20', Finish='2009-05-30', Duration='10s')
    ])
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", text='Duration', width=700,
                      labels={'Task': ''}, )
    fig.update_layout({
        "plot_bgcolor": "#fff",
        'showlegend': False,
        'margin': {
            "l": 0,  # left
            "r": 5,  # right
            "t": 5,  # top
            "b": 25,  # bottom
        }
    }).update_yaxes(autorange="reversed")
    fig.write_image(f"{file_path}.svg")


def produce_vertical_bar_chart_stacked():
    df = pd.DataFrame([
        [1, 0, "mapped"],
        [1, 100, "unmapped"],
        [2, 50, "mapped"],
        [2, 50, "unmapped"],
        [3, 100, "mapped"],
        [3, 0, "unmapped"],
    ], columns=["step", "ratio", "status"])

    fig = px.bar(df,
                 x="step",
                 y="ratio",
                 labels={'step': 'Step', 'status': 'Status'},
                 color="status",
                 text="ratio"
                 )
    fig \
        .update_layout(plot_bgcolor="#fff") \
        .update_xaxes({"tickmode": "linear"})
    fig.write_image("plotly_vertical_bar_stacked.svg")


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


def produce_horizontal_bar_chart():
    df = pd.DataFrame([
        ["MONDO", 10],
        ["MEDDRA", 50],
        ["MESH", 100],
    ], columns=["namespace", "freq %"])

    fig = px.bar(df,
                 x="freq %",
                 y="namespace",
                 text="freq %",
                 orientation='h'
                 )
    fig \
        .update_layout(plot_bgcolor="#fff") \
        .update_yaxes({"tickmode": "linear"})
    fig.write_image("plotly_horizontal_bar.svg")


def produce_horizontal_bar_chart_node_types():
    df = pd.DataFrame([
        [1, 20, "Seed (20%)"],
        [1, 30, "Merged (30%)"],
        [1, 20, "Connected (20%)"],
        [1, 30, "Dangling (30%)"],
    ], columns=["step", "ratio", "status"])

    fig = px.bar(df,
                 y="step",
                 x="ratio",
                 text="status",
                 color="status",
                 orientation='h',
                 width=1150,
                 height=50,
                 labels={'ratio': 'Input nodes'},
                 )
    fig.update_layout({
        "plot_bgcolor": "#fff",
        'showlegend': False,
        'margin': {
            "l": 35,  # left
            "r": 0,  # right
            "t": 10,  # top
            "b": 10,  # bottom
        }
    }).update_xaxes(visible=False) \
        .update_yaxes(visible=False) \
        .update_traces(textposition='inside', textfont_size=14, textfont_color="white")
    fig.write_image("plotly_horizontal_bar_chart_node_types.svg")


def produce_horizontal_bar_chart_merged_ns():
    print("produce_horizontal_bar_chart_merged_ns")
    df = pd.DataFrame([
        ["ORPHANET", "MONDO", 8585, 38.31391975721873],
        ["MESH", "MONDO", 7827, 34.93104833311019],
        ["EFO", "MONDO", 2500, 11.157227652073013],
        ["MESH", "FOO", 7827, 34.93104833311019],
        ["EFO", "FOO", 2500, 11.157227652073013],
        ["EFO", "BAR2", 2500, 11.157227652073013],
        ["EFO", "BAR1", 2500, 11.157227652073013],
        ["EFO", "BAR3", 2500, 11.157227652073013],
    ], columns=["merged", "canonical", "count", "freq"])

    # ["step", "ratio", "status"]

    fig = px.bar(df,
                 y="canonical",
                 x="count",
                 text="merged",
                 color="merged",
                 orientation='h',
                 width=700,
                 labels={
                     'canonical': 'Canonical',
                     'merged': 'Merged'
                 },
                 )
    fig.update_layout({
        # "plot_bgcolor": "#fff",
        'margin': {
            "l": 5,  # left
            "r": 5,  # right
            "t": 5,  # top
            "b": 5,  # bottom
        },
        'yaxis': {
            'autorange': 'reversed'
        }
    }).update_traces(textposition='inside',
                     textfont_size=14,
                     textfont_color="white")
    fig.write_image("plotly_horizontal_bar_chart_node_merges.svg")


def prod_heatmap(data: DataFrame, file_path: str):
    data2 = data.copy()
    print(data2)
    cols = list(data.columns)
    data2 = data2[cols].replace({'0': np.nan, 0: np.nan})
    print(data2)
    fig = px.imshow(data2,
                    text_auto=True,
                    color_continuous_scale="blues") \
        .update_xaxes(side="top") \
        .update_layout({"plot_bgcolor": "#fff"})
    fig.write_image(file_path)
    # .update_xaxes(title_font=dict(size=18, family='Courier', color='crimson'))


# produce_gantt_chart(file_path="plotly_gant_processing", data=[])
# produce_vertical_bar_chart_stacked()
# produce_vertical_bar_chart()
# produce_horizontal_bar_chart()
# produce_edge_heat_map()
# produce_horizontal_bar_chart_node_types()
# prod_heat_map()
# produce_horizontal_bar_chart_merged_ns()
produce_vertical_bar_chart_node_ns()
