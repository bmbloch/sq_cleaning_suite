import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import dash_table

def get_surv_bench_layout():
    return \
        html.Div([
            dcc.Location(id='benchmarks_home_url',pathname='/home'),
            dcc.Store('sector'),
            dcc.Store('curryr'),
            dcc.Store('currmon'),
            dcc.Store('program'),
            dcc.Store('store_user'),
            dcc.Store('trunc_cols'),
            dcc.Loading(
                        id='loading_spinner', 
                        children=html.Div(id="loading-benchmarks"),
                        fullscreen=True,
                        type='circle'
                        ),
                html.Div([
                    dbc.Alert(
                        html.P(id='benchmarks_load_alert_text'),
                        id = "benchmarks_load_alert",
                        dismissable=True,
                        is_open=False,
                        fade=False,
                    )
                ], style={'text-align': 'center', 'vertical-align': 'middle'}),
        ])