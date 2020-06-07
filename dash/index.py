import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from app import app
from panels import leads
import pandas as pd

server = app.server

app.layout = html.Div(
    [
        html.Div(
            className="row header",
            children=[
                html.Span(
                    className="app-title",
                    children=[
                        dcc.Markdown('''

                        ## TheHotelChecker

                        This website provides a data visualisation for hotel owners to gain descriptive analytics on their hotels as well as their competitors.  

                        
                        To use, select a hotel from the map to see the average score, and select a date to also see more insight on the reviews on the hotel.
                        '''
                        ),
                    ],
                ),
            ],
        ),
        dcc.Store(  # leads df
            id="leads_df", data=pd.DataFrame([['Alex', 10], ['Bob', 12], ['Clarke', 13]], columns=['Name', 'Age']).to_json(orient="split")
        ),
        dcc.Location(id="url", refresh=False),
        html.Div(id="tab_content"),
        html.Link(
            href="https://use.fontawesome.com/releases/v5.2.0/css/all.css",
            rel="stylesheet",
        ),
        html.Link(
            href="https://fonts.googleapis.com/css?family=Dosis", rel="stylesheet"
        ),
        html.Link(
            href="https://fonts.googleapis.com/css?family=Open+Sans", rel="stylesheet"
        ),
        html.Link(
            href="https://fonts.googleapis.com/css?family=Ubuntu", rel="stylesheet"
        ),
    ],
    className="row",
    style={"margin": "0%"},
)

# Update the index
@app.callback(
    [
        Output("tab_content", "children"),
        Output("tabs", "children"),
        Output("mobile_tabs", "children"),
    ],
    [Input("url", "pathname")],
)
def display_page(pathname):
    tabs = [
        dcc.Link("Leads", href="/dash-salesforce-crm/leads"),
    ]
    tabs[0] = dcc.Link(
        dcc.Markdown("**&#9632 Leads**"), href="/dash-salesforce-crm/leads"
    )
    return leads.layout, tabs, tabs


@app.callback(
    Output("mobile_tabs", "style"),
    [Input("menu", "n_clicks")],
    [State("mobile_tabs", "style")],
)
def show_menu(n_clicks, tabs_style):
    if n_clicks:
        if tabs_style["display"] == "none":
            tabs_style["display"] = "flex"
        else:
            tabs_style["display"] = "none"
    return tabs_style


if __name__ == "__main__":
    app.run_server(debug=True)
