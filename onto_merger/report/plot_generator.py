
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


def produce_edge_heat_map():
    df = px.data.medals_wide(indexed=True)
    print(df.head(10))
    print(list(df))
    print(list(df.index))
    fig = px.imshow(df)
    fig.write_image("plotly_heat_map.svg")
    # fig.show()


# produce_vertical_bar_chart()

produce_edge_heat_map()
