import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import dash_table

def get_flag_program_layout(sector_val, curryr, currmon):
    
    sector_long = {'apt': 'Apartment', 'ind': 'Industrial', 'off': 'Office', 'ret': 'Retail'}
    navbar_title = sector_long[sector_val] + " " + "Square Pool Review " + str(curryr) + "m" + str(currmon)

    navbar = dbc.Navbar(
    [
        html.Div([
            html.Div([
                html.P(navbar_title)
                ], style={'font-size': '24px', 'width': '450', 'display': 'inline-block'}),
            html.Div([  
                dbc.Row(
                dbc.Col(
                    dbc.Button('Export MSQ',id='download-button',color='primary',block=True, size='sm'),
                    width=20
                        ),
                    justify='center'
                        ),
                    ], style={'display': 'inline-block', 'padding-left': '260px'}),  
            html.Div([
                dbc.Row(
                dbc.Col(
                    dbc.Button('Export Flags',id='flag-button',color='warning',block=True, size='sm'),
                    width=20
                        ),
                    justify='center'
                        ),
                    ], style={'display': 'inline-block', 'padding-left': '50px'}),
            html.Div([
                dbc.Row(
                dbc.Col(
                    dbc.Button('Finalize MSQS',id='finalize-button',color='success',block=True, size='sm'),
                    width=20
                        ),
                    justify='center'
                        ),
                    ], style={'display': 'inline-block', 'padding-left': '50px'}),
            html.Div([
                dbc.Row(
                dbc.Col(
                    dbc.Button('Logout',id='logout-button',color='danger',block=True, size='sm'),
                    width=20
                        ),
                    justify='center'
                        ),
                    ], style={'display': 'inline-block', 'padding-left': '50px'}),
        ], style={'padding-left': '750px'}),
    ],
    fixed='top'
    )

    return \
        html.Div([
            dcc.Location(id='flags_home_url',pathname='/home'),
            dcc.Store('sector'),
            dcc.Store('curryr'),
            dcc.Store('currmon'),
            dcc.Store('program'),
            dcc.Store('store_user'),
            dcc.Store('flag_file_name'),
            dcc.Store('store_new_id'),
            dcc.Store('flags_processed'),
            dcc.Store('trunc_cols'),
            dcc.Store('flag_cols'),
            dcc.Store('out_flag_trigger'),
            dcc.Store('out_msq_trigger'),
            dcc.Store('edits_processed'),
            dcc.Store('first_roll_load', data=True),
            dcc.Loading(
                        id='loading_spinner', 
                        children=html.Div(id="flags_processed"),
                        fullscreen=True,
                        type='circle'
                        ),
            html.Div([
                        dcc.ConfirmDialog(
                        id='confirm_finalizer',
                        displayed=False,
                        message="Clicking OK will finalize the MSQs and overwrite all files currently in the output folder"
                        ),
                    ]),
            html.Div([
                dbc.Alert(
                    html.P(id='logic_alert_text'),
                    id = "finalizer_logic_alert",
                    dismissable=True,
                    is_open=False,
                    fade=False,
                    color='danger',
                        )
                    ], style={'text-align': 'center', 'vertical-align': 'middle', 'padding-top': '100px'}),
            dcc.Tabs(id ='tab_clicked', value ='flags', children=[
                dcc.Tab(label='Flags', value='flags', children=[
                    html.Div([
                        dbc.Alert(
                            html.P(id='no_flag_alert_text'),
                            id = "no_flag_alert",
                            dismissable=True,
                            is_open=False,
                            fade=False,
                            color="success"
                                )
                            ], style={'text-align': 'center', 'vertical-align': 'middle'}),
                    html.Div([
                        dash_table.DataTable(
                            id='flag_table',
                            merge_duplicate_headers=True,
                            style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto', 'font-size': '12px'},
                            page_size=30,
                            filter_action="custom",
                            sort_action="native",
                            fixed_rows={'headers': True}, # This will contstrain the height of the table to about 15 rows unfortunately
                            style_cell={'textAlign': 'center'},
                            style_table={'height': '930px', 'overflowY': 'auto'}, #Reminder that its 30 px per row, so 10 rows would be 330 (include 30 for header). So set to 930 to display 30 rows
                                            ),
                            ]),
                    html.Div([
                        dbc.Row(
                            dbc.Col(
                                dbc.Button('Set ID Review',id='ids-button',color='success',block=True,size='sm'),
                                    width=20
                                    ),
                                    ),
                            ], style={'display': 'none'}, id='ids_button_container'),
                    html.Div([
                        dash_table.DataTable(
                            id='p_val_flags',
                            merge_duplicate_headers=True,
                            style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto', 'font-size': '12px'},
                            page_size=30,
                            filter_action="native",
                            sort_action="native",
                            fixed_rows={'headers': True},
                            style_cell={'textAlign': 'center'},
                            style_table={'height': '180px', 'overflowY': 'auto'}, 
                            style_cell_conditional=[
                                                {'if': {'column_id': 'identity'}, 'width': '12%'},
                                                {'if': {'column_id': 'p_value'}, 'width': '12%'},
                                                ],
                                            ),
                            ], style={'display': 'none'}, id='pv_flag_container'),
                        ]),
                dcc.Tab(label='Filter IDs', value='ids', children=[
                        html.Div([
                            html.Div([
                                html.Div([
                                    dcc.Dropdown(
                                        id='msq_sub_drop',
                                                ),
                                        ], style={'display': 'none'}, id='msq_sub_drop_container'),
                                html.Div([
                                    dbc.Checklist(
                                        id = 'show_rent_cols',
                                        value=['r'],
                                        options=[{'label': 'Rent Cols', 'value': 'r'}],
                                        inline=True,  
                                                ),
                                        ], style={'display': 'inline-block', 'vertical-align': 'top', 'padding-left': '10px'}),
                                html.Div([
                                    dbc.Checklist(
                                        id = 'show_vac_cols',
                                        value='',
                                        options=[{'label': 'Vac Cols', 'value': 'v'}],
                                        inline=True,  
                                                ),
                                        ], style={'display': 'inline-block', 'vertical-align': 'top', 'padding-left': '10px'})
                                    ], style={'display': 'block'}),
                                html.Div([
                                    dash_table.DataTable(
                                        id='msq_table',
                                        merge_duplicate_headers=True,
                                        style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                        page_action='none',
                                        filter_action="custom",
                                        sort_action="native",
                                        fixed_rows={'headers': True},
                                                        ),
                                            ], style={'display': 'none'}, id='msq_table_container'),
                                ]),
                            html.Div([
                                html.Div([ 
                                    dbc.Row(
                                        dbc.Col(
                                            dbc.Button('Submit Fix',id='submit-button',color='success',block=True,size='sm'),
                                                width=20
                                                ),
                                        justify='center'
                                                ),
                                        ], style={'display': 'inline-block', 'padding-left': '30px'}, id='submit_button_container'),
                                html.Div([ 
                                    dbc.Row(
                                        dbc.Col(
                                            dbc.Button('Preview Fix',id='preview-button',color='warning',block=True,size='sm'),
                                                width=20
                                                ),
                                        justify='center'
                                                ),
                                        ], style={'display': 'inline-block', 'padding-left': '60px'}, id='preview_button_container'),
                                html.Div([
                                    dcc.RadioItems(
                                        id='subsequent_fix',
                                        value='q',
                                        options=[
                                                    {'label': 'msq', 'value': 'q'},
                                                    {'label': 'man', 'value': 'm'},
                                                ],
                                        labelStyle={'display': 'inline-block', 'margin': '0 15px 0 0'}), 
                                        ], style={'display': 'inline-block', 'padding-left': '60px'}, id='subsequent_change_container'),
                            ], style={'display': 'block', 'padding-top': '20px'}),
                        html.Div([
                            html.Div([
                                dash_table.DataTable(
                                    id='p_vals',
                                    merge_duplicate_headers=True,
                                    page_action='none',
                                    style_table={'height': '210px', 'overflowY': 'auto'},
                                    fixed_rows={'headers': True},
                                    style_cell={'textAlign': 'right'},
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                    style_cell_conditional=[
                                                {'if': {'column_id': 'identity'}, 'width': '12%'},
                                                {'if': {'column_id': 'cov perc'}, 'width': '12%'},
                                                {'if': {'column_id': 'level'}, 'width': '12%'},
                                                {'if': {'column_id': 'p_value'}, 'width': '12%'},
                                                {'if': {'column_id': 'sq mean'}, 'width': '12%'},
                                                {'if': {'column_id': 'surv mean'}, 'width': '12%'},
                                                {'if': {'column_id': 'total surv props'}, 'width': '12%'}
                                                ],
                                                    ),
                                    ], style={'display': 'none'}, id='pv_container'),
                            html.Div([
                                dcc.Graph(
                                    id='g_dist_graph',
                                    config={'displayModeBar': False}
                                        ),
                                    ], style={'display': 'none'}, id='g_dist_container'),
                                ], style={'display': 'block'}),
                        ]),
                    dcc.Tab(label='Rollups', value='rollups', children=[
                        html.Div([
                            dcc.Dropdown(
                                id='roll_drop',
                                        ),
                                ], style={'display': 'none'}, id='roll_drop_container'),
                        html.Div([
                            html.Div([
                                dash_table.DataTable(
                                    id='sub_roll_survey',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                    page_action='none',
                                    sort_action="native",
                                    fixed_rows={'headers': True},
                                    style_cell_conditional=[
                                                {'if': {'column_id': 'identity'}, 'width': '12%'},
                                                {'if': {'column_id': 'avg g monthized'}, 'width': '17%'},
                                                {'if': {'column_id': 'sd g monthized'}, 'width': '17%'},
                                                {'if': {'column_id': 'survs up'}, 'width': '12%'},
                                                {'if': {'column_id': 'survs down'}, 'width': '12%'},
                                                {'if': {'column_id': 'survs flat'}, 'width': '12%'},
                                                {'if': {'column_id': 'surv perc'}, 'width': '12%'},
                                                ],
                                                    ),
                                        ], style={'display': 'block', 'padding-top': '20px'}, id='sub_surv_roll_container'),
                            html.Div([
                                dash_table.DataTable(
                                    id='met_roll_survey',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                    fixed_rows={'headers': True},
                                    style_cell_conditional=[
                                                            {'if': {'column_id': 'identity'}, 'width': '12%'},
                                                            {'if': {'column_id': 'avg g monthized'}, 'width': '17%'},
                                                            {'if': {'column_id': 'sd g monthized'}, 'width': '17%'},
                                                            {'if': {'column_id': 'survs up'}, 'width': '12%'},
                                                            {'if': {'column_id': 'survs down'}, 'width': '12%'},
                                                            {'if': {'column_id': 'survs flat'}, 'width': '12%'},
                                                            {'if': {'column_id': 'surv perc'}, 'width': '12%'},
                                                            ],
                                                    ),
                                        ], style={'display': 'block', 'padding-top': '30px'}, id='met_surv_roll_container'),
                            html.Div([
                                dash_table.DataTable(
                                    id='us_roll_survey',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                    fixed_rows={'headers': True},
                                    style_cell_conditional=[
                                                            {'if': {'column_id': 'subsector'}, 'width': '12%'},
                                                            {'if': {'column_id': 'avg g monthized'}, 'width': '17%'},
                                                            {'if': {'column_id': 'sd g monthized'}, 'width': '17%'},
                                                            {'if': {'column_id': 'survs up'}, 'width': '12%'},
                                                            {'if': {'column_id': 'survs down'}, 'width': '12%'},
                                                            {'if': {'column_id': 'survs flat'}, 'width': '12%'},
                                                            {'if': {'column_id': 'surv perc'}, 'width': '12%'},
                                                            ],
                                                    ),
                                        ], style={'display': 'block', 'padding-top': '30px'}, id='us_surv_roll_container'),
                                ], style={'display': 'inline-block', 'padding-left': '15px', 'width': '50%'}),
                        html.Div([
                            html.Div([
                                dash_table.DataTable(
                                    id='sub_roll_square',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                    page_action='none',
                                    sort_action="native",
                                    fixed_rows={'headers': True},
                                    style_cell_conditional=[
                                                {'if': {'column_id': 'identity'}, 'width': '12%'},
                                                {'if': {'column_id': 'sub rent g surv'}, 'width': '17%'},
                                                {'if': {'column_id': 'sub rent g sq'}, 'width': '17%'},
                                                {'if': {'column_id': 'sub rent g all'}, 'width': '17%'},
                                                {'if': {'column_id': 'props up'}, 'width': '12%'},
                                                {'if': {'column_id': 'props down'}, 'width': '12%'},
                                                {'if': {'column_id': 'props flat'}, 'width': '12%'},
                                                ],
                                                    ),
                                        ], style={'display': 'block', 'padding-top': '20px'}, id='sub_sq_roll_container'),
                            html.Div([
                                dash_table.DataTable(
                                    id='met_roll_square',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                    fixed_rows={'headers': True},
                                    style_cell_conditional=[
                                                            {'if': {'column_id': 'identity'}, 'width': '12%'},
                                                            {'if': {'column_id': 'met rent g surv'}, 'width': '17%'},
                                                            {'if': {'column_id': 'met rent g sq'}, 'width': '17%'},
                                                            {'if': {'column_id': 'met rent g all'}, 'width': '17%'},
                                                            {'if': {'column_id': 'props up'}, 'width': '12%'},
                                                            {'if': {'column_id': 'props down'}, 'width': '12%'},
                                                            {'if': {'column_id': 'props flat'}, 'width': '12%'},
                                                            ],
                                                    ),
                                        ], style={'display': 'block', 'padding-top': '30px'}, id='met_sq_roll_container'),
                            html.Div([
                                dash_table.DataTable(
                                    id='us_roll_square',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                    fixed_rows={'headers': True},
                                    style_cell_conditional=[
                                                            {'if': {'column_id': 'subsector'}, 'width': '12%'},
                                                            {'if': {'column_id': 'us rent g surv'}, 'width': '17%'},
                                                            {'if': {'column_id': 'us rent g sq'}, 'width': '17%'},
                                                            {'if': {'column_id': 'us rent g all'}, 'width': '17%'},
                                                            {'if': {'column_id': 'props up'}, 'width': '12%'},
                                                            {'if': {'column_id': 'props down'}, 'width': '12%'},
                                                            {'if': {'column_id': 'props flat'}, 'width': '12%'},
                                                            ],
                                                    ),
                                        ], style={'display': 'block', 'padding-top': '30px'}, id='us_sq_roll_container'),
                                ], style={'display': 'inline-block', 'padding-left': '20px', 'padding-right': '15px', 'width': '50%', 'vertical-align': 'top'}),
                        ]),
                ], style={'padding-top': '50px'}),
            html.Div([navbar]), 
        ]),