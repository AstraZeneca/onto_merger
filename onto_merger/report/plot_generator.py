
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def produce_gant_chart():
    df = pd.DataFrame([
        dict(Task="Job A", Start='2009-01-01', Finish='2009-02-28'),
        dict(Task="Job B", Start='2009-03-05', Finish='2009-04-15'),
        dict(Task="Job C", Start='2009-02-20', Finish='2009-05-30')
    ])

    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", width=700)
    fig.update_yaxes(autorange="reversed")

    fig.write_image("plotly_gant_processing.svg")


def produce_vertical_bar_chart():

    x = ['b', 'a', 'c', 'd']
    fig = go.Figure(go.Bar(x=x, y=[2, 5, 1, 9], name='Montreal'))

    fig.update_layout(barmode='stack',
                      yaxis={'categoryorder': 'array',
                             'categoryarray': ['d', 'a', 'c', 'b']})
    fig.write_image("plotly_vertical_bar.svg")


def prod_heat_map():
    idx = ['MONDO', 'MEDDRA', 'MESH', 'DOID']
    cols = ['MONDO', 'MEDDRA', 'MESH', 'DOID']
    df = pd.DataFrame([[100, 20, 30, 40],
                       [50, 100, 8, 15],
                       [25, 14, 100, 8],
                       [7, 14, 21, 100]],
                      columns=cols, index=idx)
    fig = px.imshow(df, text_auto=True, color_continuous_scale="blues").update_xaxes(side="top")
    fig.write_image("plotly_heat_map.svg")



# produce_vertical_bar_chart()

# produce_edge_heat_map()
prod_heat_map()