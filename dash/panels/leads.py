# -*- coding: utf-8 -*-
import pandas as pd
import json
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
from plotly import graph_objs as go
import ast
from pyspark.sql import functions as F
from datetime import datetime as dt

from app import reviews, app, indicator, df_to_table


def hotel_map(status):
    mapbox_access_token = open(".mapbox_token").read()

    markers = reviews.select("hotel_name", "lat", "lng").distinct().collect()
    names = [str(row.hotel_name) for row in markers]
    lat = [float(row.lat) for row in markers]
    lng = [float(row.lng) for row in markers]

    trace = go.Scattermapbox(
        lat=lat,
        lon=lng,
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=9,
            symbol='lodging',
            color='blue'
        ),
        text=names
    )

    layout = go.Layout(
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            accesstoken=mapbox_access_token,
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=48.864716,
                lon=2.349014
            ),
            pitch=0,
            zoom=12
        ),
        mapbox_style="light",
        margin=dict(l=0, r=0, t=0, b=0),
        clickmode='event+select'
    )
    return dict(data=[trace], layout=layout)


# returns pie chart that shows lead source repartition
def lead_source(selectedData, startDate, endDate):
    if selectedData is None:
        trace = go.Pie(
            labels=["-"],
            values=[1],
            marker={"colors": ["#264e86"]},
        )

        layout = dict(autosize=True, margin=dict(l=15, r=10, t=0, b=65))
        return dict(data=[trace], layout=layout)
    else:
        temp = ast.literal_eval(str(selectedData))
        selected_hotel_name = temp['points'][0]['text']
        selected_hotel = reviews.select("tags").filter(
            reviews["hotel_name"] == selected_hotel_name).take(1)

        tags = ast.literal_eval(selected_hotel[0].tags)
        values = [4500, 2500, 1053, 500]

        trace = go.Pie(
            labels=tags,
            values=values,
            marker={"colors": ["#264e86", "#0074e4", "#74dbef", "#eff0f4"]},
        )

        layout = dict(autosize=True, margin=dict(l=15, r=10, t=0, b=65))
        return dict(data=[trace], layout=layout)


def converted_leads_count(selectedData, startDate, endDate):
    if selectedData is None or startDate is None or endDate is None:
        trace = go.Bar(x=[], y=[])

        layout = go.Layout(
            autosize=True,
            xaxis=dict(showgrid=False),
            margin=dict(l=33, r=25, b=37, t=5, pad=4),
            paper_bgcolor="white",
            plot_bgcolor="white",
        )
        return {"data": [trace], "layout": layout}
    else:
        temp = ast.literal_eval(str(selectedData))
        selected_hotel_name = temp['points'][0]['text']

        positive_reviews = reviews \
            .select("hotel_name", F.date_format('review_date', 'yyyy-MM-dd').alias('day')) \
            .filter(reviews["hotel_name"] == selected_hotel_name) \
            .filter((F.col('day') >= startDate) & (F.col('day') <= endDate)) \
            .filter(F.col('sentiment') == 1) \
            .groupby('day') \
            .count() \
            .select("day", F.col("count").alias("n")) \
            .orderBy('day', ascending=True) \
            .collect()

        days = [str(row.day) for row in positive_reviews]
        n = [int(row.n) for row in positive_reviews]

        positive_trace = go.Bar(name="positive reviews", x=days, y=n, marker_color='green')

        negative_reviews = reviews \
            .select("hotel_name", F.date_format('review_date', 'yyyy-MM-dd').alias('day')) \
            .filter(reviews["hotel_name"] == selected_hotel_name) \
            .filter((F.col('day') >= startDate) & (F.col('day') <= endDate)) \
            .filter(F.col('sentiment') == 0) \
            .groupby('day') \
            .count() \
            .select("day", F.col("count").alias("n")) \
            .orderBy('day', ascending=True) \
            .collect()

        days = [str(row.day) for row in negative_reviews]
        n = [int(row.n) for row in negative_reviews]

        negative_trace = go.Bar(name="negative reviews", x=days, y=n, marker_color='red')

        layout = go.Layout(
            autosize=True,
            xaxis=dict(showgrid=False),
            margin=dict(l=33, r=25, b=37, t=5, pad=4),
            paper_bgcolor="white",
            plot_bgcolor="white",
        )
        return {"data": [positive_trace, negative_trace], "layout": layout}


layout = [
    html.Div(
        id="lead_grid",
        children=[
            html.Div(
                className="two columns dd-styles",
                children=dcc.DatePickerRange(
                    id='my-date-picker-range',
                    min_date_allowed=dt(2010, 8, 5),
                    max_date_allowed=dt(2020, 5, 19),
                    initial_visible_month=dt(2017, 8, 5),
                    start_date=dt(2017, 8, 25).date()
                ),
            ),
            html.Div(
                className="row indicators",
                children=[
                    indicator("#00cc96", "Hotel",
                              "left_leads_indicator"),
                    indicator("#119DFF", "Score",
                              "middle_leads_indicator"),
                ],
            ),
            html.Div(
                id="leads_per_state",
                className="chart_div pretty_container",
                children=[
                    html.P("Hotels"),
                    dcc.Graph(
                        id="map",
                        style={"height": "90%", "width": "98%"},
                        config=dict(displayModeBar=False),
                    ),
                ],
            ),
            html.Div(
                id="leads_source_container",
                className="six columns chart_div pretty_container",
                children=[
                    html.P("Most common tags"),
                    dcc.Graph(
                        id="lead_source",
                        style={"height": "90%", "width": "98%"},
                        config=dict(displayModeBar=False),
                    ),
                ],
            ),
            html.Div(
                id="converted_leads_container",
                className="six columns chart_div pretty_container",
                children=[
                    html.P("Reviews per day"),
                    dcc.Graph(
                        id="converted_leads",
                        style={"height": "90%", "width": "98%"},
                        config=dict(displayModeBar=False),
                    ),
                ],
            ),
            html.Div(id="leads_table", className="row pretty_container table"),
        ],
    ),
]


# updates left indicator based on df updates
@app.callback(Output("left_leads_indicator", "children"),
              [Input("map", "clickData")])
def left_leads_indicator_callback(selectedData):
    if selectedData is None:
        return dcc.Markdown("-")
    temp = ast.literal_eval(str(selectedData))
    selected_hotel_name = temp['points'][0]['text']
    # selected_hotel = reviews.select("hotel_name").filter(reviews["hotel_name"]==selected["points"]["text"]).take(1)
    return dcc.Markdown("**{}**".format(selected_hotel_name))


# updates middle indicator based on df updates
@app.callback(Output("middle_leads_indicator", "children"), [Input("map", "clickData")])
def middle_leads_indicator_callback(selectedData):
    if selectedData is None:
        return dcc.Markdown("-")
    temp = ast.literal_eval(str(selectedData))
    selected_hotel_name = temp['points'][0]['text']
    selected_hotel = reviews.select("average_score").filter(
        reviews["hotel_name"] == selected_hotel_name).take(1)
    return dcc.Markdown("**{}**".format(selected_hotel[0].average_score))


# updates right indicator based on df updates
@app.callback(Output("right_leads_indicator", "children"))
def right_leads_indicator_callback(df):
    return dcc.Markdown("**{}**".format(0))


# update pie chart figure based on dropdown's value and df updates
@app.callback(
    Output("lead_source", "figure"),
    [Input("map", "clickData"),
     Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')],
)
def lead_source_callback(selectedData, startDate, endDate):
    return lead_source(selectedData, startDate, endDate)

# update heat map figure based on dropdown's value and df updates


@app.callback(
    Output("map", "figure"),
    [Input('my-date-picker-range', 'start_date')],
)
def map_callback(status):
    return hotel_map(status)

# update table based on dropdown's value and df updates


@app.callback(
    Output("leads_table", "children"),
    [Input("map", "clickData"),
     Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')],
)
def leads_table_callback(selectedData, startDate, endDate):
    if selectedData is None or startDate is None or endDate is None:
        return None
    else:
        temp = ast.literal_eval(str(selectedData))
        selected_hotel_name = temp['points'][0]['text']

        selected_hotel = reviews \
            .select(F.date_format('review_date', 'yyyy-MM-dd').alias('Date'), reviews['review'].alias("Review"), reviews['sentiment'].alias("Sentiment"), reviews['tags'].alias("Tags")) \
            .filter(reviews["hotel_name"] == selected_hotel_name) \
            .filter((F.col('Date') >= startDate) & (F.col('Date') <= endDate)) \
            .withColumn("Sentiment", F.when(F.col("Sentiment") == 1, "Positive").when(F.col("Sentiment") == 0, "negative")) \
            .toPandas()
        return df_to_table(selected_hotel.head(5))


# update pie chart figure based on dropdown's value and df updates
@app.callback(
    Output("converted_leads", "figure"),
    [Input("map", "clickData"),
     Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')],
)
def converted_leads_callback(selectedData, startDate, endDate):
    return converted_leads_count(selectedData, startDate, endDate)
