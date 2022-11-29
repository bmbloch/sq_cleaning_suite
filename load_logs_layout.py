import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import dash_table

def get_load_logs_layout():
    return \
        html.Div([
            dcc.Location(id='load_logs_home_url',pathname='/home'),
            dcc.Store('sector'),
            dcc.Store('curryr'),
            dcc.Store('currmon'),
            dcc.Store('program'),
            dcc.Store('store_user'),
            dcc.Store('store_log_errors'),
            dcc.Store('trunc_cols'),
            dcc.Loading(
                        id='loading_spinner', 
                        children=html.Div(id="loading-logs"),
                        fullscreen=True,
                        type='circle'
                        ),
                html.Div([
                    dbc.Alert(
                        html.P(id='log_load_alert_text'),
                        id = "log_load_alert",
                        dismissable=True,
                        is_open=False,
                        fade=False,
                    )
                ], style={'text-align': 'center', 'vertical-align': 'middle'}),
                html.Div([
                    dbc.Alert(
                        html.P(id='log_errors_alert_text'),
                        id = "log_errors_alert",
                        dismissable=True,
                        is_open=False,
                        fade=False,
                    )
                ], style={'text-align': 'center', 'vertical-align': 'middle'}),

        ])