import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import dash_table

def get_exam_survs_layout():
    return \
        html.Div([
            dcc.Location(id='exam_survs_url',pathname='/home'),
            dcc.Store('sector'),
            dcc.Store('curryr'),
            dcc.Store('currmon'),
            dcc.Store('program'),
            dcc.Store('store_user'),
            dcc.Store('trunc_cols'),
            dcc.Store('struct_cols'),
            dcc.Store('first_trigger', data=False),
            dcc.Loading(
                        id='loading_spinner', 
                        children=html.Div(id="loading-survs"),
                        fullscreen=True,
                        type='circle'
                        ),
            html.Div([
                dbc.Row(
                dbc.Col(
                    dbc.Button('Logout',id='logout-button',color='danger',block=True, size='sm'),
                    width=20
                        ),
                    justify='center'
                        ),
                    ], style={'display': 'inline-block', 'padding-left': '1850px'}),
            html.Div([
                dash_table.DataTable(
                    id='surv_table',
                    merge_duplicate_headers=True,
                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto', 'font-size': '12px'},
                    page_size=30,
                    filter_action="native",
                    sort_action="native",
                    fixed_rows={'headers': True}, # This will contstrain the height of the table to about 15 rows unfortunately
                    style_cell={'textAlign': 'center'},
                    style_table={'height': '480px', 'overflowY': 'auto'}, 
                    ),
                    ]),
            html.Div([
                dash_table.DataTable(
                    id='secondary_table',
                    merge_duplicate_headers=True,
                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto', 'font-size': '12px'},
                    style_cell={'textAlign': 'center'},
                    style_table={'height': '60px'}, 
                    ),
                    ]),
                ]),
 