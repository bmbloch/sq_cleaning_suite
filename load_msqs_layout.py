import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import dash_table

def get_load_msq_layout():
    return \
        html.Div([
            dcc.Location(id='load_msq_home_url',pathname='/home'),
            dcc.Store('sector'),
            dcc.Store('curryr'),
            dcc.Store('currmon'),
            dcc.Store('program'),
            dcc.Store('store_user'),
            dcc.Store('trunc_cols'),
            dcc.Loading(
                        id='loading_spinner', 
                        children=html.Div(id="loading-msqs"),
                        fullscreen=True,
                        type='circle'
                        ),
                html.Div([
                    dbc.Alert(
                        html.P(id='msq_load_alert_text'),
                        id = "msq_load_alert",
                        dismissable=True,
                        is_open=False,
                        fade=False,
                    )
                ], style={'text-align': 'center', 'vertical-align': 'middle'}),
                html.Div([
                        dcc.ConfirmDialog(
                        id='confirm_msq_pickle',
                        displayed=False,
                        message="Clicking OK will refresh the MSQs to match the OOB values, overwriting any edits already made. Are you sure you want to proceed?"
                        ),
                    ]),

        ])