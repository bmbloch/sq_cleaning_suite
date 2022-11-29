import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash import no_update
import plotly.graph_objs as go 
import plotly.figure_factory as ff
from flask import session, copy_current_request_context
import dash_table
import dash_table.FormatTemplate as FormatTemplate
from dash_table.Format import Format, Scheme
from dash.exceptions import PreventUpdate
import urllib
import os
from os import listdir
from os.path import isfile, join
import numpy as np
import pandas as pd
import csv
from pathlib import Path
import re
from datetime import datetime
import math
import time
import scipy.stats as st
from random import sample
import multiprocessing as mp
import timeit
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.4f}'.format
from IPython.core.display import display, HTML

from timer import Timer
from auth_sq_support import authenticate_user, validate_login_session
from server_sq_support import sq_support, server
from login_layout_sq_support import get_login_layout
from load_msqs_layout import get_load_msq_layout
from load_logs_layout import get_load_logs_layout
from exam_survs_layout import get_exam_survs_layout
from flag_program_layout import get_flag_program_layout
from survey_benchmarks_layout import get_surv_bench_layout
from check_input_update import check_input_freshness

from load_msqs import convert_msqs
from load_logs import load_log_files
from sq_capture_survey import sq_capture_survey_flags
from sq_logic import sq_logic_flags
from find_outdated_sublet import sublet_flags
from survey_benchmarks import process_survey_benchmarks
from outlier_r_level import outlier_r_level_flags
from ren_g import ren_g_flags
from g_dist import g_dist_flags
from vac_g import vac_g_flags
from opex_rent_relationship import opex_rent_flags
from tax_opex_relationship import tax_opex_flags
from find_new_survs import get_surv_changes
from sq_hist import sq_hist_flags
from exam_survs import search_logs_load


def get_home():
    if os.name == "nt": return "//odin/reisadmin/"
    else: return "/home/"

def get_input_id():
    ctx = dash.callback_context

    if not ctx.triggered:
        button_id = 'No update yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        

    return button_id

def get_style(dataframe, style_type):
    if style_type == "partial":
        style = [ 
                    {
                        'if': {'column_id': str(x), 'filter_query': '{{{0}}} < 0'.format(x)},
                        'color': 'red',
                    } for x in dataframe.columns
                ]
    elif style_type == "full":
        style = [ 
                    {
                        'if': {'column_id': str(x), 'filter_query': '{{{0}}} < 0'.format(x)},
                        'color': 'red',
                    } for x in dataframe.columns

                ] + [
                    {'if': 
                        {'row_index': 'odd'}, 
                            'backgroundColor': 'rgb(248, 248, 248)'
                    }
                ]

    return style

def get_types(sector_val, flag_cols=[]):

    type_dict = {}
    format_dict = {}

    for x in flag_cols:
        type_dict[x] = 'numeric'
        format_dict[x] = Format(precision=0, scheme=Scheme.fixed)
    
    type_dict['identity'] = 'text'
    type_dict['submarket'] = 'text'
    type_dict['subsector'] = 'text'
    type_dict['sector'] = 'text'
    type_dict['metro'] = 'text'
    type_dict['Identity'] = 'text'
    type_dict['Benchmark'] = 'text'
    type_dict['rnt term'] = 'text'
    type_dict['type2'] = 'text'
    type_dict['source'] = 'text'
    type_dict['surstat'] = 'text'
    type_dict['metcode'] = 'text'
    type_dict['submkt'] = 'text'
    type_dict['propname'] = 'text'
    type_dict['address'] = 'text'
    type_dict['city'] = 'text'
    type_dict['county'] = 'text'
    type_dict['state'] = 'text'
    type_dict['status'] = 'text'
    type_dict['class'] = 'text'
    type_dict['lease own'] = 'text'
    type_dict['code out'] = 'text'
    type_dict['flag period'] = 'text'
    type_dict['level'] = 'text'
    type_dict['estunit'] = 'text'
    type_dict['type1'] = 'text'
    type_dict['property source id'] = 'text'
    
    type_dict['g_renx'] = 'numeric'
    type_dict['g_vacx'] = 'numeric'
    type_dict['sq_g_renx'] = 'numeric'
    type_dict['sq_g_vacx'] = 'numeric'
    type_dict['surv_g_renx'] = 'numeric'
    type_dict['surv_g_vacx'] = 'numeric'
    type_dict['met_ranking'] = 'numeric'
    type_dict['met_rank_rent'] = 'numeric'
    type_dict['sub_rank_rent'] = 'numeric'
    type_dict['met_rank_avail'] = 'numeric'
    type_dict['sub_rank_avail'] = 'numeric'
    type_dict['Sub Surveyed Mean'] = 'numeric'
    type_dict['Met Surveyed Mean'] = 'numeric'
    type_dict['Nat Surveyed Mean'] = 'numeric'
    type_dict['Sub Coverage Percent'] = 'numeric'
    type_dict['Met Coverage Percent'] = 'numeric'
    type_dict['Nat Coverage Percent'] = 'numeric'
    type_dict['P Value'] = 'numeric'
    type_dict['Surveyed Props'] = 'numeric'
    type_dict['id'] = 'numeric'
    type_dict['d_surv_mean'] = 'numeric'
    type_dict['yr'] = 'numeric'
    type_dict['qtr'] = 'numeric'
    type_dict['currmon'] = 'numeric'
    type_dict['ind size'] = 'numeric'
    type_dict['ind avail'] = 'numeric'
    type_dict['yearx'] = 'numeric'
    type_dict['month'] = 'numeric'
    type_dict['renx'] = 'numeric'
    type_dict['renxM'] = 'numeric'
    type_dict['opex'] = 'numeric'
    type_dict['opexM'] = 'numeric'
    type_dict['taxx'] = 'numeric'
    type_dict['taxxM'] = 'numeric'
    type_dict['availx'] = 'numeric'
    type_dict['availxM'] = 'numeric'
    type_dict['vacratx'] = 'numeric'
    type_dict['totavailx'] = 'numeric'
    type_dict['totvacx'] = 'numeric'
    type_dict['sublet'] = 'numeric'
    type_dict['subletxM'] = 'numeric'
    type_dict['termx'] = 'numeric'
    type_dict['termxM'] = 'numeric'
    type_dict['realid'] = 'numeric'
    type_dict['phase'] = 'numeric'
    type_dict['survmon'] = 'numeric'
    type_dict['realyr'] = 'numeric'
    type_dict['realqtr'] = 'numeric'
    type_dict['newprop'] = 'numeric'
    type_dict['existsx'] = 'numeric'
    type_dict['existcon'] = 'numeric'
    type_dict['subid'] = 'numeric'
    type_dict['zip'] = 'numeric'
    type_dict['fipscode'] = 'numeric'
    type_dict['yearxM'] = 'numeric'
    type_dict['renov'] = 'numeric'
    type_dict['expren'] = 'numeric'
    type_dict['sizex'] = 'numeric'
    type_dict['sizexM'] = 'numeric'
    type_dict['off size'] = 'numeric'
    type_dict['contig'] = 'numeric'
    type_dict['flrsx'] = 'numeric'
    type_dict['flrsxM'] = 'numeric'
    type_dict['bldgs'] = 'numeric'
    type_dict['parkx'] = 'numeric'
    type_dict['parkxM'] = 'numeric'
    type_dict['lowrent'] = 'numeric'
    type_dict['hirent'] = 'numeric'
    type_dict['avrent'] = 'numeric'
    type_dict['c rent'] = 'numeric'
    type_dict['ti1'] = 'numeric'
    type_dict['ti2'] = 'numeric'
    type_dict['ti renew'] = 'numeric'
    type_dict['free re'] = 'numeric'
    type_dict['comm1'] = 'numeric'
    type_dict['comm2'] = 'numeric'
    type_dict['op exp'] = 'numeric'
    type_dict['lossx'] = 'numeric'
    type_dict['lossxM'] = 'numeric'
    type_dict['x'] = 'numeric'
    type_dict['y'] = 'numeric'
    type_dict['d2cbd'] = 'numeric'
    type_dict['d2hiway'] = 'numeric'
    type_dict['d2landmk'] = 'numeric'
    type_dict['density'] = 'numeric'
    type_dict['distwt'] = 'numeric'
    type_dict['conv yr'] = 'numeric'
    type_dict['re tax'] = 'numeric'
    type_dict['basetax'] = 'numeric'
    type_dict['premtax'] = 'numeric'
    type_dict['baseren'] = 'numeric'
    type_dict['premren'] = 'numeric'
    type_dict['baseope'] = 'numeric'
    type_dict['premope'] = 'numeric'
    type_dict['baseterm'] = 'numeric'
    type_dict['premterm'] = 'numeric'
    type_dict['basevac'] = 'numeric'
    type_dict['premvac'] = 'numeric'
    type_dict['basetotvac'] = 'numeric'
    type_dict['premtotvac'] = 'numeric'
    type_dict['offsizex'] = 'numeric'
    type_dict['totsizex'] = 'numeric'
    type_dict['lse term'] = 'numeric'
    type_dict['Gmrent'] = 'numeric'
    type_dict['vac chg'] = 'numeric'
    type_dict['avg g monthized'] = 'numeric'
    type_dict['sd g monthized'] = 'numeric'
    type_dict['survs up'] = 'numeric'
    type_dict['survs down'] = 'numeric'
    type_dict['survs flat'] = 'numeric'
    type_dict['props up'] = 'numeric'
    type_dict['props down'] = 'numeric'
    type_dict['props flat'] = 'numeric'
    type_dict['sub rent g surv'] = 'numeric'
    type_dict['met rent g surv'] = 'numeric'
    type_dict['us rent g surv'] = 'numeric'
    type_dict['sub rent g sq'] = 'numeric'
    type_dict['met rent g sq'] = 'numeric'
    type_dict['us rent g sq'] = 'numeric'
    type_dict['sub rent g all'] = 'numeric'
    type_dict['met rent g all'] = 'numeric'
    type_dict['us rent g all'] = 'numeric'
    type_dict['surv perc'] = 'numeric'
    type_dict['p_value'] = 'numeric'
    type_dict['cov perc'] = 'numeric'
    type_dict['total surv props'] = 'numeric'
    type_dict['sq mean'] = 'numeric'
    type_dict['surv mean'] = 'numeric'
    type_dict['ind avrent'] = 'numeric'
    type_dict['n size'] = 'numeric'
    type_dict['a size'] = 'numeric'
    type_dict['n avail'] = 'numeric'
    type_dict['a avail'] = 'numeric'
    type_dict['n avrent'] = 'numeric'
    type_dict['a avrent'] = 'numeric'
    type_dict['size'] = 'numeric'
    type_dict['avrent'] = 'numeric'
    type_dict['avail'] = 'numeric'
    type_dict['totunits'] = 'numeric'
    type_dict['ren0'] = 'numeric'
    type_dict['ren1'] = 'numeric'
    type_dict['ren2'] = 'numeric'
    type_dict['ren3'] = 'numeric'
    type_dict['ren4'] = 'numeric'
    type_dict['vac0'] = 'numeric'
    type_dict['vac1'] = 'numeric'
    type_dict['vac2'] = 'numeric'
    type_dict['vac3'] = 'numeric'
    type_dict['vac4'] = 'numeric'
    type_dict['avgren'] = 'numeric'
    type_dict['year'] = 'numeric'
    type_dict['docks'] = 'numeric'
    type_dict['dockhigh doors'] = 'numeric'
    type_dict['drivein doors'] = 'numeric'
    type_dict['rail doors'] = 'numeric'
    type_dict['ceil avg'] = 'numeric'
    type_dict['flrs'] = 'numeric'
    type_dict['units0'] = 'numeric'
    type_dict['units1'] = 'numeric'
    type_dict['units2'] = 'numeric'
    type_dict['units3'] = 'numeric'
    type_dict['units4'] = 'numeric'
    type_dict['tot size'] = 'numeric'


    type_dict['survdate'] = 'datetime'

            
    format_dict['identity'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['submarket'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['sector'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['metro'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['met_ranking'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['met_rank_rent'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['sub_rank_rent'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['met_rank_avail'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['sub_rank_avail'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Surveyed Props'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Identity'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Benchmark'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['id'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['yr'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['qtr'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['currmon'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['yearx'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['month'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['renxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['opexM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['taxxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['availxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['subletxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['termxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['rnt term'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['survdate'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['type2'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['source'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['surstat'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['metcode'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['submkt'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['propname'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['address'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['city'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['county'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['state'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['status'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['class'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['realid'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['phase'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['survmon'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['realyr'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['realqtr'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['newprop'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['existsx'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['existcon'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['subid'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['zip'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['fipscode'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['yearxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['renov'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['lease own'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['sizexM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['flrsx'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['flrsxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['bldgs'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['parkx'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['parkxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['conv yr'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['code out'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['expren'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['lossx'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['lossxM'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['distwt'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['flag period'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['survs up'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['survs down'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['survs flat'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['props up'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['props down'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['props flat'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['subsector'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['level'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['total surv props'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['year'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['docks'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['dockhigh doors'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['drivein doors'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['rail doors'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['flrs'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['units0'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['units1'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['units2'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['units3'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['units4'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['estunit'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['type1'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['property source id'] = Format(precision=0, scheme=Scheme.fixed)

    format_dict['ren0'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['ren1'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['ren2'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['ren3'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['ren4'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['ceil avg'] = Format(precision=1, scheme=Scheme.fixed)

    format_dict['renx'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['opex'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['taxx'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['lowrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['hirent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['avrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['c rent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['ti1'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['ti2'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['ti renew'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['free re'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['comm1'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['comm2'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['op exp'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['re tax'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['basetax'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['baseren'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['baseope'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['termx'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['baseterm'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['lse term'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['p_value'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['ind avrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['n avrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['a avrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['avrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['avgren'] = Format(precision=2, scheme=Scheme.fixed)
    
    format_dict['P Value'] = Format(precision=3, scheme=Scheme.fixed)
    format_dict['d2cbd'] = Format(precision=3, scheme=Scheme.fixed)
    format_dict['d2hiway'] = Format(precision=3, scheme=Scheme.fixed)
    format_dict['d2landmk'] = Format(precision=3, scheme=Scheme.fixed)
    format_dict['density'] = Format(precision=3, scheme=Scheme.fixed)

    format_dict['x'] = Format(precision=5, scheme=Scheme.fixed)
    format_dict['y'] = Format(precision=5, scheme=Scheme.fixed)


    format_dict['g_renx'] = FormatTemplate.percentage(1)
    format_dict['g_vacx'] = FormatTemplate.percentage(1)
    format_dict['sq_g_renx'] = FormatTemplate.percentage(1)
    format_dict['sq_g_vacx'] = FormatTemplate.percentage(1)
    format_dict['surv_g_renx'] = FormatTemplate.percentage(1)
    format_dict['surv_g_vacx'] = FormatTemplate.percentage(1)
    format_dict['d_surv_mean'] = FormatTemplate.percentage(1)
    format_dict['Sub Surveyed Mean'] = FormatTemplate.percentage(1)
    format_dict['Met Surveyed Mean'] = FormatTemplate.percentage(1)
    format_dict['Nat Surveyed Mean'] = FormatTemplate.percentage(1)
    format_dict['Sub Coverage Percent'] = FormatTemplate.percentage(1)
    format_dict['Met Coverage Percent'] = FormatTemplate.percentage(1)
    format_dict['Nat Coverage Percent'] = FormatTemplate.percentage(1)
    format_dict['surv perc'] = FormatTemplate.percentage(1)
    format_dict['cov perc'] = FormatTemplate.percentage(1)


    format_dict['vacratx'] = FormatTemplate.percentage(2)
    format_dict['totvacx'] = FormatTemplate.percentage(2)
    format_dict['premtax'] = FormatTemplate.percentage(2)
    format_dict['premren'] = FormatTemplate.percentage(2)
    format_dict['premope'] = FormatTemplate.percentage(2)
    format_dict['premterm'] = FormatTemplate.percentage(2)
    format_dict['basevac'] = FormatTemplate.percentage(2)
    format_dict['premvac'] = FormatTemplate.percentage(2)
    format_dict['basetotvac'] = FormatTemplate.percentage(2)
    format_dict['premtotvac'] = FormatTemplate.percentage(2)
    format_dict['Gmrent'] = FormatTemplate.percentage(2)
    format_dict['vac chg'] = FormatTemplate.percentage(2)
    format_dict['avg g monthized'] = FormatTemplate.percentage(2)
    format_dict['sd g monthized'] = FormatTemplate.percentage(2)
    format_dict['sub rent g surv'] = FormatTemplate.percentage(2)
    format_dict['met rent g surv'] = FormatTemplate.percentage(2)
    format_dict['us rent g surv'] = FormatTemplate.percentage(2)
    format_dict['sub rent g sq'] = FormatTemplate.percentage(2)
    format_dict['met rent g sq'] = FormatTemplate.percentage(2)
    format_dict['us rent g sq'] = FormatTemplate.percentage(2)
    format_dict['sub rent g all'] = FormatTemplate.percentage(2)
    format_dict['met rent g all'] = FormatTemplate.percentage(2)
    format_dict['us rent g all'] = FormatTemplate.percentage(2)
    format_dict['sq mean'] = FormatTemplate.percentage(2)
    format_dict['surv mean'] = FormatTemplate.percentage(2)

    format_dict['ind size'] = Format(group=",")
    format_dict['availx'] = Format(group=",")
    format_dict['totavailx'] = Format(group=",")
    format_dict['sublet'] = Format(group=",")
    format_dict['sizex'] = Format(group=",")
    format_dict['off size'] = Format(group=",")
    format_dict['contig'] = Format(group=",")
    format_dict['offsizex'] = Format(group=",")
    format_dict['totsizex'] = Format(group=",")
    format_dict['ind avail'] = Format(group=",")
    format_dict['n size'] = Format(group=",")
    format_dict['a size'] = Format(group=",")
    format_dict['n avail'] = Format(group=",")
    format_dict['a avail'] = Format(group=",")
    format_dict['size'] = Format(group=",")
    format_dict['avail'] = Format(group=",")
    format_dict['totunits'] = Format(group=",")
    format_dict['vac0'] = Format(group=",")
    format_dict['vac1'] = Format(group=",")
    format_dict['vac2'] = Format(group=",")
    format_dict['vac3'] = Format(group=",")
    format_dict['vac4'] = Format(group=",")
    format_dict['tot size'] = Format(group=",")


    return type_dict, format_dict

def use_pickle(direction, file_name, dataframe, curryr, currmon, sector_val):
        base_file_path = Path("{}central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles".format(get_home(), sector_val, curryr, currmon))
        
        if direction == "in":
            data = pd.read_pickle(base_file_path / (file_name + '.pickle'))
            return data
        elif direction == "out":
            dataframe.to_pickle(base_file_path / (file_name + '.pickle'))

def process_filter(data_in, filter_in, flag_cols=[]):

    data = data_in.copy()

    int_list = ['id', 'yr', 'qtr', 'currmon', 'ind_size', 'yearx', 'month', 'renxM', 'opexM', 'taxxM', 'availx', 'availxM', 'totavailx', 'sublet', 'subletxM', 'termxM']
    float_list = ['renx', 'opex', 'taxx', 'vacratx', 'totvacx', 'termx']
    
    if "&&" in filter_in:
        filter_col = []
        filter_val = []
        filter_split = filter_in.split(" && ")
        for elem in filter_split:
            col, val = elem.split(' = ')
            col = col[1:-1]
            filter_col.append(col)
            filter_val.append(val)
    else:
        col, val = filter_in.split(' = ')
        col = col[1:-1]
        filter_col = [col]
        filter_val = [val]

    for col, val in zip(filter_col, filter_val):
        if col in int_list or col in flag_cols:
            val = int(val)
        elif col in float_list:
            val = float(val)
            
        data = data[data[col] == val]

    return data

def aggreg_msqs(sector_val, trunc_cols, period, file_path):
    base_file_path = Path("{}central/square/data/zzz-bb-test2/python/sq_redev/{}/{}/{}".format(get_home(), sector_val, period, file_path))
    msq = pd.read_pickle(base_file_path)
    
    if period != "trunc_msqs":
        msq = msq[trunc_cols + ['identity']]
    else:
        msq = msq[trunc_cols]

    return msq

def set_pool(sector_val, trunc_cols, period):

    root = Path("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}".format(sector_val, period))
    file_list = [f for f in listdir(root) if isfile(join(root, f))]
    
    pool = mp.Pool(int(mp.cpu_count()*0.7))
    result_async = [pool.apply_async(aggreg_msqs, args = (sector_val, trunc_cols, period, path, )) for path in
                    file_list]
    results = [r.get() for r in result_async]

    msq_data = pd.DataFrame()
    msq_data = msq_data.append(results, ignore_index=True)
    msq_data['type2_temp'] = np.where(msq_data['type2'] == "F", "F", "DW")
    msq_data.sort_values(by=['type2_temp', 'metcode', 'subid', 'id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True, True, True], inplace=True)
    msq_data['identity_met'] = msq_data['metcode'] + msq_data['type2_temp']
    msq_data = msq_data.reset_index(drop=True)

    pool.close()

    if period != "trunc_msqs":
        msq_list = msq_data.copy()
        rent_comps_only_list = ['BD', 'CG', 'DC', 'HL', 'SS']
        msq_list = msq_list[(msq_list['subid'] != 77) | (msq_list['metcode'].isin(rent_comps_only_list))]
        msq_list = msq_list[msq_list['submkt'].str.contains('99') == False]
        sub_list = list(msq_list['identity'].unique())
        met_list = list(msq_list['identity_met'].unique())
        msq_data = msq_data.drop(['identity', 'identity_met', 'type2_temp'], axis=1)
    else:
        msq_data = msq_data.drop(['identity_met', 'type2_temp'], axis=1)
        sub_list = []
        met_list = []

    return msq_data, sub_list, met_list

def rename_cols(program, flag_data, flag_cols):

    if program == "sq_capture":
        for col in list(flag_data.columns):
            if "_" in col:
                flag_data.rename(columns={col: col.replace('_', ' ').replace('r flag', '').replace('v flag', '').lstrip().rstrip()}, inplace=True)

        flag_cols = [x.replace('_', ' ').replace('r flag', '').replace('v flag', '').lstrip().rstrip() for x in flag_cols]
    
    elif program == "sq_logic":
        for col in list(flag_data.columns):
            if "_" in col:
                flag_data.rename(columns={col: col.replace('_', ' ').replace('r flag', '').replace('v flag', '').replace('negative', 'neg').replace('missing', 'm').lstrip().rstrip()}, inplace=True)
        flag_cols = [x.replace('_', ' ').replace('r flag', '').replace('v flag', '').replace('negative', 'neg').replace('missing', 'm').lstrip().rstrip() for x in flag_cols]

    elif program == "outlier_level":
        for col in list(flag_data.columns):
            if "_" in col:
                flag_data.rename(columns={col: col.replace('_', ' ').replace('r flag', '').replace('v flag', '').lstrip().rstrip()}, inplace=True)
        flag_cols = [x.replace('_', ' ').replace('r flag', '').replace('v flag', '').lstrip().rstrip() for x in flag_cols]

    elif program == "surv_chgs":
        for col in list(flag_data.columns):
            if "_" in col:
                flag_data.rename(columns={col: col.replace('_', ' ').lstrip().rstrip()}, inplace=True)
        flag_cols = [x.replace('_', ' ').lstrip().rstrip() for x in flag_cols]

    elif program == "out_sublet":
        for col in list(flag_data.columns):
            if "_" in col:
                flag_data.rename(columns={col: col.replace('_', ' ').lstrip().rstrip()}, inplace=True)
        flag_cols = [x.replace('_', ' ').lstrip().rstrip() for x in flag_cols]

    elif program == "ren_g" or program == "vac_g" or "or_rel" or "tax_opex" or program == 'sq_hist':
        for col in list(flag_data.columns):
            if "_" in col:
                flag_data.rename(columns={col: col.replace('_', ' ').lstrip().rstrip()}, inplace=True)
        flag_cols = [x.replace('_', ' ').lstrip().rstrip() for x in flag_cols]

    return flag_data, flag_cols


# Layout for login page
def login_layout():
    return get_login_layout()

# Main page layout
@validate_login_session
def app_layout(program, sector_val, curryr, currmon):
    if program == "load_msqs":
        return get_load_msq_layout()
    elif program == "load_logs":
        return get_load_logs_layout()
    elif program == "surv_bench":
        return get_surv_bench_layout()
    elif program == 'exam_survs':
        return get_exam_survs_layout()
    else:
        return get_flag_program_layout(sector_val, curryr, currmon)

# Full multipage app layout
sq_support.layout = html.Div([
                    dcc.Location(
                        id='url',
                        refresh=False
                                ),
                    html.Div(
                            login_layout(),
                            id='page-content',                      
                                ),
                            ])

# All program callbacks 

# Check to see what url the user entered into the web browser, and return the relevant page based on their choice
@sq_support.callback([Output('page-content','children'),
                     Output('msq_fresh_check', 'is_open'),
                     Output('msq_fresh_check_text', 'children')],
                     [Input('url','pathname')],
                     [State('program_drop', 'value'),
                     State('login-curryr', 'value'),
                     State('login-currmon', 'value'),
                     State('sector_input', 'value'),])

def router(pathname, program, curryr, currmon, sector_input):
    if currmon is None or sector_input is None:
        return login_layout(), no_update, no_update
    else:
        curryr = int(curryr)
        currmon = int(currmon)

        if program != 'exam_survs':
            file_status = check_input_freshness(sector_input, curryr, currmon, program)
        else:
            file_status = 'refreshed'

        if file_status == "refreshed" and program == "sq_hist":
            isFile = os.path.isfile('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/new_survs.pickle'.format(sector_input, curryr, currmon))
            if isFile == False: 
                file_status = 'new_rents_needed'

        if file_status == "refreshed" or program == "load_msqs" or program == "load_logs":
            if pathname[0:5] == '/home':
                return app_layout(program, sector_input, curryr, currmon), no_update, no_update
            elif pathname == '/login':
                return login_layout(), no_update, no_update
            else:
                return login_layout(), no_update, no_update
        else:
            if file_status == "oob":
                message = "The OOB MSQs have not yet been updated. Wait for them to be processed before starting Square Pool Cleaning"
            elif file_status == "new_rents_needed":
                message = "Run the Find Surv Changes program first before running SQ Hist Changes"
            else:
                if file_status == "msq":
                    file_name = "Load MSQs"
                elif file_status == "logs":
                    file_name = "Load Logs"
                elif file_status == "bench":
                    file_name = "Survey Benchmarks"
                message= "Run {} first before using the other SQ Pool Cleaning programs".format(file_name)

            return login_layout(), True, message

# Authenticate by checking credentials, if correct, authenticate the session, if not, authenticate the session and send user to login
@sq_support.callback([Output('url','pathname'),
                     Output('login-alert','children'),
                     Output('url', 'search')],
                     [Input('login-button','n_clicks')],
                     [State('login-username','value'),
                     State('login-password','value'),
                     State('login-curryr', 'value'),
                     State('login-currmon', 'value'),
                     State('sector_input', 'value'),
                     State('program_drop', 'value')])
def login_auth(n_clicks, username, pw, curryr, currmon, sector_input, program):
    if n_clicks is None or n_clicks==0:
        return '/login', no_update, ''
    else:
        credentials = {'user': username, "password": pw, "sector": sector_input}
        if authenticate_user(credentials) == True and sector_input is not None and program is not None:
            session['authed'] = True
            pathname = '/home' + "?"
            return pathname, '', username + "/" + sector_input.title() + "/" + str(curryr) + "m" + str(currmon) + "/" + program
        else:
            session['authed'] = False
            if sector_input == None:
                message = 'Select a Sector.'
            elif program == None:
                message = "Select a Program"
            else:
                message = 'Incorrect credentials.'
            return no_update, dbc.Alert(message, color='danger', dismissable=True), no_update


@sq_support.callback([Output('store_user', 'data'),
                      Output('sector', 'data'),
                      Output('curryr', 'data'),
                      Output('currmon', 'data'),
                      Output('program', 'data'),
                      Output('trunc_cols', 'data')],
                      [Input('url', 'search')])
def store_input_vals(url_input):
    if url_input is None:
        raise PreventUpdate
    else:
        user, sector_val, global_vals, program = url_input.split("/")
        curryr, currmon = global_vals.split("m")
        curryr = int(curryr)
        currmon = int(currmon)

        # If update this list, also need to update the list in load_msqs.py
        if program != 'exam_survs':
            msq_trunc_cols = ['id', 'realid', 'yr', 'qtr', 'currmon', 'type2', 'metcode', 'subid', 'submkt', 'survdate', 'yearx', 'month', 'avrent', 're_tax', 'op_exp', 'rnt_term', 'renx', 'renxM',  'taxx', 'taxxM', 'opex', 'opexM', 'availx', 'availxM', 'totavailx', 'sublet', 'subletxM', 'ind_size', 'vacratx', 'totvacx', 'termx', 'termxM']
        else:
            msq_trunc_cols = []
        return user, sector_val.lower(), curryr, currmon, program, msq_trunc_cols

@sq_support.callback(Output('out_msq_trigger', 'data'),
                     [Input('download-button', 'n_clicks'),
                     Input('flags_processed', 'data'),
                     Input('submit-button', 'data')],
                     [State('curryr', 'data'), 
                      State('currmon', 'data'),
                      State('sector', 'data'),
                      State('msq_sub_drop', 'value')])

def export_current_msq(n_clicks, flags_processed, submit_button, curryr, currmon, sector_val, current_sub):
    if sector_val is None:
        raise PreventUpdate
    else:
        input_id = get_input_id()
        if input_id == "download-button":
            
            msq_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs/{}msq.pickle'.format(sector_val, current_sub[0:2].lower()))
            msq_data.to_csv('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/current_msq_data.csv'.format(sector_val, curryr, currmon), index=False)

            return True
        else:
            no_update

@sq_support.callback(Output('out_flag_trigger', 'data'),
                     [Input('flag-button', 'n_clicks'),
                     Input('flags_processed', 'data')],
                     [State('flag_file_name', 'data'),
                      State('curryr', 'data'), 
                      State('currmon', 'data'),
                      State('sector', 'data')])

def export_flags(n_clicks, flags_processed, flag_file_name, curryr, currmon, sector_val):
    if flag_file_name is None:
        raise PreventUpdate
    else:
        input_id = get_input_id()
        if input_id == "flag-button":
            
            flags = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/{}.pickle'.format(sector_val, curryr, currmon, flag_file_name))
            flags.to_csv('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/{}.csv'.format(sector_val, curryr, currmon, flag_file_name), index=False)

            return True
        else:
            no_update


@sq_support.callback(Output('confirm_finalizer', 'displayed'),
                [Input('sector', 'data'),
                Input('edits_processed', 'data'),
                Input('finalize-button', 'n_clicks')],
                [State('curryr', 'data'),
                State('currmon', 'data')])
def confirm_finalizer(sector_val, edits_processed, finalize_button, curryr, currmon):
    input_id = get_input_id()

    if sector_val is None:
        raise PreventUpdate
    # Need this callback to tie to finalize_edits callback so the callback is not executed before the data is actually updated, but only want to actually save the data when the finalize button is clicked, so only do that when the input id is for the finalize button
    elif input_id != "finalize-button":
        raise PreventUpdate
    else:
        return True

@sq_support.callback([Output('finalizer_logic_alert', 'is_open'),
                     Output('logic_alert_text', 'children')],
                     [Input('confirm_finalizer', 'submit_n_clicks'),
                     Input('flags_processed', 'data')],
                     [State('curryr', 'data'), 
                      State('currmon', 'data'),
                      State('sector', 'data'),
                      State('trunc_cols', 'data')])

def finalize_msqs(confirm_click, flags_processed, curryr, currmon, sector_val, trunc_cols):
    if confirm_click is None:
        raise PreventUpdate
    else:
        msq_data, sub_list, met_list = set_pool(sector_val, trunc_cols, "curr_msqs")
        flag_cols = sq_logic_flags(sector_val, curryr, currmon, msq_data, True)
        flags = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sq_logic.pickle'.format(sector_val, curryr, currmon))
        if len(flags) > 0:
            alert = True
            flagged_subs = list(flags['identity'].unique())
            alert_text = 'There are logic flags in the following submarkets. Re-run sq_logic and address the flags before the MSQs can be finalized: \n\n' + ', '.join(map(str, flagged_subs))
        else:
            root = Path("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs".format(sector_val))
            file_list = [f for f in listdir(root) if isfile(join(root, f))]

            for msq in file_list:
                msq.to_stata('/home/central/square/data/{}/production/msq/output/{}.dta'.format(msq[:-7]))

            alert = False
            alert_text = ''
        
        return alert, alert_text

@sq_support.callback([Output('msq_table', 'data'),
                      Output('msq_table', 'columns'),
                      Output('msq_table', 'style_data'),
                      Output('msq_table_container', 'style'),
                      Output('msq_table', 'style_table'), 
                      Output('msq_table', 'style_cell_conditional'),
                      Output('msq_table', 'style_cell'),
                      Output('p_vals', 'data'),
                      Output('p_vals', 'columns'),
                      Output('pv_container', 'style'),
                      Output('g_dist_graph', 'figure'),
                      Output('g_dist_container', 'style')],
                      [Input('msq_sub_drop', 'value'),
                      Input('msq_table', 'filter_query'),
                      Input('show_rent_cols', 'value'),
                      Input('show_vac_cols', 'value'),
                      Input('flags_processed', 'data')],
                      [State('flag_file_name', 'data'),
                      State('curryr', 'data'), 
                      State('currmon', 'data'),
                      State('sector', 'data'),
                      State('flag_table', 'selected_row_ids')])

def filter_msq_table(sub_drop, filter_in, show_rent, show_vac, flags_processed, flag_file_name, curryr, currmon, sector_val, selected_row_ids):
    if sub_drop is None:
        raise PreventUpdate
    else:
        input_id = get_input_id()
        if selected_row_ids is None:
            selected_row_ids = []
        msq_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs/{}msq.pickle'.format(sector_val, sub_drop[0:2].lower()))
        msq_id_list = msq_data['id'].unique()

        data_title = msq_data.reset_index().loc[0]['submkt'] + " Submarket Data"

        if flag_file_name == 'ren_g':
            sq_reng = msq_data.copy()
            sq_reng = sq_reng[sq_reng['identity'] == sub_drop]
            sq_reng = sq_reng[sq_reng['renxM'] == 1]
            sq_reng = sq_reng[(sq_reng['yr'] == curryr) & (sq_reng['currmon'] == currmon)]
            sq_reng['sub_live'] = np.where(sq_reng['submkt'].str[0:2] == "99", 0, 1)
            rent_comps_only_list = ['BD', 'CG', 'DC', 'HL', 'SS']
            sq_reng = sq_reng[sq_reng['sub_live'] == 1]
            sq_reng = sq_reng[(sq_reng['subid'] != 77) | (sq_reng['metcode'].isin(rent_comps_only_list))]
            
            sq_rents = list(sq_reng['Gmrent'])

            surv_data = pd.read_pickle("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/renx_surv_data.pickle".format(sector_val, curryr, currmon))
            surv_data = surv_data[surv_data['identity'] == sub_drop]
            surv_rents = list(surv_data['g_renx'])
        
        base_cols = ['id', 'yr', 'qtr', 'currmon', 'survdate', 'ind_size', 'yearx', 'month']
        rent_cols = ['rnt_term', 'renx', 'renxM', 'Gmrent', 'opex', 'opexM', 'taxx', 'taxxM']
        vac_cols = ['availx', 'availxM', 'vacratx', 'totavailx', 'totvacx', 'vac_chg', 'sublet', 'subletxM']
        
        if 'r' in show_rent or 'v' in show_vac:
            filt_cols = []
            if 'r' in show_rent:
                filt_cols += rent_cols
            if 'v' in show_vac:
                filt_cols += vac_cols
            msq_data = msq_data[base_cols + filt_cols]
        
        for col in list(msq_data.columns):
            if "_" in col:
                msq_data.rename(columns={col: col.replace('_', ' ')}, inplace=True)

        
        if (filter_in is None or filter_in == '') and len(selected_row_ids) == 0:
            first_id = msq_data.reset_index().loc[0]['id']
            msq_data = msq_data[msq_data['id'] == first_id]
        elif filter_in is not None and filter_in != '':
            msq_data = process_filter(msq_data, filter_in)
        elif len(selected_row_ids) > 0:
            if selected_row_ids[0] in msq_id_list:
                msq_data = msq_data[msq_data['id'] == selected_row_ids[0]]
            else:
                first_id = msq_data.reset_index().loc[0]['id']
                msq_data = msq_data[msq_data['id'] == first_id]
            

        highlighting_msq_data = get_style(msq_data, "partial")
        type_dict_data, format_dict_data = get_types(sector_val)

        msq_data_style = {'display': 'block'}

        edit_dict = {}
        no_edit_cols = ['id', 'yr', 'qtr', 'currmon', 'survdate']
        for x in list(msq_data.columns):
            if x in no_edit_cols:
                edit_dict[x] = False
            else:
                edit_dict[x] = True

        if show_rent == 'r' or show_vac == 'v':
            style_table = {'height': '930px', 'overflowY': 'auto'}
        else:
            style_table = {'height': '930px', 'overflowY': 'auto', 'overflowX': 'auto'}

        dict_val = {}
        style_cell_conditional = []

        adjust_dict = {'totavailx': '5%',
                        'subletxM': '5%'
                        }

        if 'r' in show_rent or 'v' in show_vac:
            base = '3%'
        else:
            base = '5%'
        for x in list(msq_data.columns):
            if x not in list(adjust_dict.keys()):
                temp_dict = {'if': {'column_id': x}, 'width': base}
            else:
                temp_dict = {'if': {'column_id': x}, 'width': adjust_dict[x]}
            style_cell_conditional.append(temp_dict)

        if 'r' in show_rent or 'v' in show_vac:
            style_cell={'textAlign': 'center'}
        else:
            style_cell={'textAlign': 'center', 
                        'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 
                        'textOverflow': 'ellipsis'}

        if flag_file_name != 'ren_g':
            p_values = no_update
            p_val_cols = no_update
            p_values_style = no_update
            graph_style = no_update
            fig_rg = no_update
            p_val_table = no_update
        elif flag_file_name == 'ren_g':
            p_values = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/renx_p_values.pickle'.format(sector_val, curryr, currmon))
            
            p_values = p_values[p_values['identity'] == sub_drop]
            p_values = p_values.rename(columns={'cov_perc': 'cov perc', 'tot_surv_props': 'total surv props', 'sq_mean': 'sq mean'})
            if len(surv_rents) > 0:
                p_values['surv mean'] = np.mean(surv_rents)
            else:
                p_values['surv mean'] = np.nan

            p_values = p_values[['identity', 'p_value', 'level', 'total surv props', 'cov perc', 'surv mean', 'sq mean']]

            type_dict, format_dict = get_types(sector_val)
            
            p_val_table = p_values.to_dict('records')
            p_val_cols = [{'name': ['G Dist Evaluation', p_values.columns[i]], 'id': p_values.columns[i], 'type': type_dict[p_values.columns[i]], 'format': format_dict[p_values.columns[i]]}
                                 for i in range(0, len(p_values.columns))]
            
            p_values_style = {'display': 'inline-block', 'padding-left': '20px', 'width': '45%', 'padding-top': '30px'}
            
            # To avoid the situation where all data points are the same, causing an error because a curve cant be drawn, add a dummy value on either side of it that is just a bit different
            if len(surv_rents) > 0:
                if all(ele == surv_rents[0] for ele in surv_rents) == True:
                    surv_rents += [surv_rents[0] - 0.001, surv_rents[0] + 0.001]
            if len(sq_rents) > 0:
                if all(ele == sq_rents[0] for ele in sq_rents) == True:
                    sq_rents += [sq_rents[0] - 0.001, sq_rents[0] + 0.001]
            if len(sq_rents) > 0 and len(surv_rents) > 0:

                group_labels = [p_values.reset_index().loc[0]['level'] + " Surveyed Ids", 'Squared Ids']

                rg_all = [surv_rents, sq_rents]

                fig_rg = ff.create_distplot(rg_all, group_labels, show_hist=False, show_rug = False)
                fig_rg.update_traces(hovertemplate='%{x}<extra></extra>')
                fig_rg.update_layout(
                    margin={'l': 70, 'b': 30, 'r': 10, 't': 70, 'pad': 20},
                    title={
                        'text': "Surveyed Vs Squared Rent Change Distribution",
                        'y':0.99,
                        'x':0.45,
                        'xanchor': 'center',
                        'yanchor': 'top'},
                    xaxis=dict(
                        title='Rent Change',
                        tickformat= ',.01%'
                        ),
                    yaxis=dict(
                        title='Kernel Density',
                        ),
                    legend=dict(
                        yanchor='top',
                        xanchor='right',
                        y=1,
                        x=1,
                        bgcolor="LightSteelBlue",
                        bordercolor="Black",
                        borderwidth=2
                        )
                    )

                graph_style = {'display': 'inline-block', 'padding-left': '50px', 'vertical-align': 'top', 'width': '45%'}

            else:
                fig_rg = no_update
                graph_style = {'display': 'none'}


        return msq_data.to_dict('records'), [{'name': [data_title, msq_data.columns[i]], 'id': msq_data.columns[i], 'type': type_dict_data[msq_data.columns[i]], 'format': format_dict_data[msq_data.columns[i]], 'editable': edit_dict[msq_data.columns[i]]} 
                            for i in range(0, len(msq_data.columns))], highlighting_msq_data, msq_data_style, style_table, style_cell_conditional, style_cell, p_val_table, p_val_cols, p_values_style, fig_rg, graph_style

@sq_support.callback([Output('flag_table', 'data'),
                      Output('flag_table', 'columns'),
                      Output('flag_table', 'style_data_conditional'),
                      Output('flag_table', 'style'),
                      Output('flag_table', 'style_cell_conditional'),
                      Output('flag_table', 'row_selectable'),
                      Output('flag_table', 'row_deletable'),
                      Output('no_flag_alert_text', 'children'),
                      Output('no_flag_alert', 'is_open'),
                      Output('ids_button_container', 'style'),
                      Output('p_val_flags', 'data'),
                      Output('p_val_flags', 'columns'),
                      Output('pv_flag_container', 'style')],
                      [Input('flags_processed', 'data'),
                      Input('flag_table', 'filter_query')],
                      [State('flag_file_name', 'data'),
                      State('curryr', 'data'), 
                      State('currmon', 'data'),
                      State('sector', 'data'),
                      State('program', 'data'),
                      State('flag_cols', 'data')])

def filter_flag_table(flags_processed, filter_in, flag_file_name, curryr, currmon, sector_val, program, flag_cols):
    
    if flags_processed is None:
        raise PreventUpdate
    else:
        flag_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/{}.pickle'.format(sector_val, curryr, currmon, flag_file_name))
        
        if flag_file_name == 'ren_g':
            p_values = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/renx_p_values.pickle'.format(sector_val, curryr, currmon))
            p_values = p_values[p_values['p_value'] >= 0.3]
            if len(p_values) > 0:
                p_values = p_values[['identity', 'p_value']]
        else:
            p_values = pd.DataFrame()

        if len(flag_data) == 0 and len(p_values) == 0:
            message = "There are no flags. Click the x to return to the program selection screen"
            alert_open = True
        
        if len(flag_data) == 0:
            style_cell_conditional = no_update
            highlighting_flag_data = no_update
            flag_data_style = no_update
            row_selectable = no_update
            row_deletable = no_update
            type_dict = {}
            format_dict = {}
            flag_table = no_update
            flag_table_cols = no_update
            set_id_button_style = {'display': 'none'}
        else:
            message = no_update
            alert_open = no_update

            if program != "outlier_level":
                flag_data = flag_data[['identity', 'id'] + flag_cols + ['flag_period']]
            else:
                flag_data = flag_data[['identity', 'id'] + flag_cols]

            flag_data, flag_cols = rename_cols(program, flag_data, flag_cols)
            
            if filter_in is not None and filter_in != '':
                flag_data = process_filter(flag_data, filter_in, flag_cols)

            type_dict, format_dict = get_types(sector_val, flag_cols)

            highlighting_flag_data = get_style(flag_data, "partial")

            flag_data_style = {'display': 'block'}
            set_id_button_style = {'display': 'block', 'padding-top': '30px', 'padding-left': '30px'}

            dict_val = {}
            style_cell_conditional = []

            for x in list(flag_data.columns):
                if x in flag_cols:
                    temp_dict = {'if': {'column_id': x}, 'width': '25px'}
                else:
                    temp_dict = {'if': {'column_id': x}, 'width': '20px'}
                style_cell_conditional.append(temp_dict)

            row_selectable = "multi"
            row_deletable = True

            flag_table = flag_data.to_dict('records')
            flag_table_cols = [{'name': flag_data.columns[i], 'id': flag_data.columns[i], 'type': type_dict[flag_data.columns[i]], 'format': format_dict[flag_data.columns[i]]}
                                 for i in range(0, len(flag_data.columns))]
        if len(p_values) == 0:
            p_val_flags = no_update
            p_val_cols = no_update
            pv_style = no_update
        else:
            message = no_update
            alert_open = no_update
            p_val_flags = p_values.to_dict('records')
            p_val_cols = [{'name': p_values.columns[i], 'id': p_values.columns[i], 'type': type_dict[p_values.columns[i]], 'format': format_dict[p_values.columns[i]]}
                                 for i in range(0, len(p_values.columns))]
            pv_style = {'display': 'block', 'padding-left': '600px', 'padding-top': '30px', 'width': '75%'}

        return flag_table, flag_table_cols, highlighting_flag_data, flag_data_style, style_cell_conditional, row_selectable, row_deletable, message, alert_open, set_id_button_style, p_val_flags, p_val_cols, pv_style

@sq_support.callback([Output('flag_table', 'selected_row_ids'),
                     Output('flag_table', 'selected_rows'),
                     Output('msq_sub_drop', 'value'),
                     Output('msq_table', 'filter_query')],
                     [Input('submit-button', 'n_clicks'),
                     Input('ids-button', 'n_clicks'),
                     Input('flags_processed', 'data')],
                     [State('sector', 'data'),
                     State('curryr', 'data'),
                     State('currmon', 'data'),
                     State('flag_file_name', 'data'),
                     State('flag_table', 'selected_row_ids'),
                     State('flag_table', 'selected_rows'),
                     State('msq_table', 'data')])

def set_msq_drop(submit_nclicks, ids_nclicks, flags_processed, sector_val, curryr, currmon, flag_file_name, selected_ids, selected_rows, edits_data):
    
    if flags_processed is None:
        raise PreventUpdate
    else:
        if selected_ids is None:
            selected_ids = []
            selected_rows = []
        
        input_id = get_input_id()
        if input_id == "submit-button":
            edit_id = edits_data[0]['id']
            if edit_id in selected_ids:
                index = selected_ids.index(edit_id)
                selected_ids.remove(edit_id)
                selected_rows.pop(index)
        
        if input_id == "flags_processed":
            flag_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/{}.pickle'.format(sector_val, curryr, currmon, flag_file_name))
            if len(flag_data) == 0:
                identity_for_drop = no_update
            else:
                identity_for_drop = flag_data.reset_index().loc[0]['identity']
            selected_ids = no_update
        else:
            if len(selected_ids) > 0:
                id_identity_list = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/id_identity_list.pickle'.format(sector_val, curryr, currmon))
                identity_for_drop = id_identity_list[id_identity_list['id'] == selected_ids[0]].reset_index().loc[0]['identity']
            else:
                flag_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/{}.pickle'.format(sector_val, curryr, currmon, flag_file_name))
                if len(flag_data) == 0:
                    identity_for_drop = no_update
                else:
                    identity_for_drop = flag_data.reset_index().loc[0]['identity']

        return selected_ids, selected_rows, identity_for_drop, ''


@sq_support.callback(Output('edits_processed', 'data'),
                     [Input('msq_table', 'data'),
                     Input('submit-button', 'n_clicks')],
                     [State('msq_sub_drop', 'value'),
                     State('sector', 'data'), 
                     State('curryr', 'data'),
                     State('currmon', 'data'),
                     State('show_rent_cols', 'value'),
                     State('show_vac_cols', 'value'),
                     State('trunc_cols', 'data')])

def finalize_edits(edits_data, n_clicks, sub_drop, sector_val, curryr, currmon, show_rent, show_vac, trunc_cols):
    if sector_val is None:
        raise PreventUpdate
    else:
        input_id = get_input_id()
        if input_id == "submit-button":

            msq_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs/{}msq.pickle'.format(sector_val, sub_drop[0:2].lower()))

            edit_id = edits_data[0]['id']
            temp = msq_data.copy()
            temp = temp[temp['id'] == edit_id]
            temp = temp.reset_index(drop=True)
            msq_data = msq_data[msq_data['id'] != edit_id]
            edits = pd.DataFrame(edits_data)
            edits = edits.reset_index(drop=True)
            
            base_cols = ['ind_size', 'yearx', 'month']
            rent_cols = ['rnt_term', 'renx', 'renxM', 'Gmrent', 'opex', 'opexM', 'taxx', 'taxxM']
            vac_cols = ['availx', 'availxM', 'vacratx', 'totavailx', 'totvacx', 'vac_chg', 'sublet', 'subletxM']

            if 'r' in show_rent and 'v' not in show_vac:
                for col in base_cols:
                    if col == "ind_size":
                        temp[col] = edits['ind size']
                    else:
                        temp[col] = edits[col]
                for col in rent_cols:
                    if col == "rnt_term":
                        temp[col] = edits['rnt term']
                    else:
                        temp[col] = edits[col]
                msq_data = msq_data.append(temp, ignore_index=True)
                msq_data.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
                msq_data = msq_data.reset_index(drop=True)
                        
            elif 'v' in show_vac and 'r' not in show_rent:
                for col in base_cols:
                    if col == "ind_size":
                        temp[col] = edits['ind size']
                    else:
                        temp[col] = edits[col]
                for col in vac_cols:
                    if col == "vac_chg":
                        temp[col] = edits['vac chg']
                    else:
                        temp[col] = edits[col]
                msq_data = msq_data.append(temp, ignore_index=True)
                msq_data.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
                msq_data = msq_data.reset_index(drop=True)

            elif 'r' in show_rent and 'v' in show_vac:
                for col in base_cols:
                    if col == "ind_size":
                        temp[col] = edits['ind size']
                    else:
                        temp[col] = edits[col]
                for col in rent_cols:
                    if col == "rnt_term":
                        temp[col] = edits['rnt term']
                    else:
                        temp[col] = edits[col]
                for col in vac_cols:
                    if col == "vac_chg":
                        temp[col] = edits['vac chg']
                    else:
                        temp[col] = edits[col]
                msq_data = msq_data.append(temp, ignore_index=True)
                msq_data.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
                msq_data = msq_data.reset_index(drop=True)
            
            else:
                for x, y in zip(msq_data.columns, edits.columns):
                    edits.rename(columns={y: x}, inplace=True)
                msq_data = msq_data.append(edits, ignore_index=True)
                msq_data.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
                msq_data = msq_data.reset_index(drop=True)

            msq_data['Gmrent'] = np.where((msq_data['id'] == msq_data['id'].shift(1)), (msq_data['renx'] - msq_data['renx'].shift(1)) / msq_data['renx'].shift(1), np.nan)
            msq_data['vac_chg'] = np.where((msq_data['id'] == msq_data['id'].shift(1)), msq_data['totvacx'] - msq_data['totvacx'], np.nan)

            msq_data.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs/{}msq.pickle'.format(sector_val, sub_drop[0:2].lower()))

            msq_trunc = msq_data.copy()
            msq_trunc = msq_trunc[trunc_cols]
            if currmon == 1:
                msq_trunc = msq_trunc[((msq_trunc['yr'] == curryr) & (msq_trunc['currmon'] == currmon)) | ((msq_trunc['yr'] == curryr - 1) & (msq_trunc['currmon'] == 12))]
            else:
                msq_trunc = msq_trunc[((msq_trunc['yr'] == curryr) & (msq_trunc['currmon'] == currmon)) | ((msq_trunc['yr'] == curryr) & (msq_trunc['currmon'] == currmon - 1))]
            msq_trunc.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/trunc_msqs/{}msq.pickle'.format(sector_val, sub_drop[0:2].lower()))

        return True

@sq_support.callback([Output('first_roll_load', 'data'),
                      Output('sub_roll_survey', 'data'),
                      Output('sub_roll_survey', 'columns'),
                      Output('sub_roll_survey', 'style_data_conditional'),
                      Output('met_roll_survey', 'data'),
                      Output('met_roll_survey', 'columns'),
                      Output('met_roll_survey', 'style_data_conditional'),
                      Output('us_roll_survey', 'data'),
                      Output('us_roll_survey', 'columns'),
                      Output('us_roll_survey', 'style_data_conditional'),
                      Output('sub_roll_square', 'data'),
                      Output('sub_roll_square', 'columns'),
                      Output('sub_roll_square', 'style_data_conditional'),
                      Output('met_roll_square', 'data'),
                      Output('met_roll_square', 'columns'),
                      Output('met_roll_square', 'style_data_conditional'),
                      Output('us_roll_square', 'data'),
                      Output('us_roll_square', 'columns'),
                      Output('us_roll_square', 'style_data_conditional')],
                      [Input('flags_processed', 'data'),
                      Input('roll_drop', 'value'),
                      Input('msq_sub_drop', 'value'),
                      Input('submit-button', 'n_clicks'),
                      Input('edits_processed', 'data')],
                      [State('program', 'data'),
                      State('sector', 'data'),
                      State('curryr', 'data'),
                      State('currmon', 'data'),
                      State('trunc_cols', 'data'),
                      State('first_roll_load', 'data')])

def rollups(flags_processed, roll_drop, temp, submit_button, edits_processed, program, sector_val, curryr, currmon, trunc_cols, first_load):
    input_id = get_input_id()
    # Note: Need to have msq_sub_drop as input here, to ensure that pickle files in this callback and in the set_msq_drop callback arent read at the same time. But prevent rollups from processing if msq_sub_drop is the actual input, unless this is the first load 
    if flags_processed is None or (input_id == "msq_sub_drop" and first_load == False):
        raise PreventUpdate
    else:

        expansion_list = ["AA", "AB", "AK", "AN", "AQ", "BD", "BF", "BI", "BR", "BS", "CG", "CM", "CN", "CS", "DC", 
                    "DM", "DN", "EP", "FC", "FM", "FR", "GD", "GN", "GR", "HR", "HL", "HT", "KX", "LL", "LO", 
                    "LR", "LV", "LX", "MW", "NF", "NM", "NO", "NY", "OK", "OM", "PV", "RE", "RO", "SC", "SH", 
                    "SR", "SS", "ST", "SY", "TC", "TM", "TO", "TU", "VJ", "VN", "WC", "WK", "WL", "WS"]

        sub_avgs_rent = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sub_avgs.pickle'.format(sector_val, curryr, currmon))
        met_avgs_rent = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/metro_avgs.pickle'.format(sector_val, curryr, currmon))
        us_avgs_rent = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/us_avgs.pickle'.format(sector_val, curryr, currmon))

        if roll_drop[0:2] in expansion_list:
            us_avgs_rent = us_avgs_rent[us_avgs_rent['tier'] == "Exp_DW"]
            us_avgs_rent['tier'] = us_avgs_rent['tier'].str[4:]
            us_avgs_rent = us_avgs_rent.rename(columns={'tier': 'type2'})
        else:
            us_avgs_rent = us_avgs_rent[us_avgs_rent['tier'] != "Exp_DW"]
            us_avgs_rent['tier'] = us_avgs_rent['tier'].str[4:]
            us_avgs_rent = us_avgs_rent.rename(columns={'tier': 'type2'})
            us_avgs_rent = us_avgs_rent[us_avgs_rent['type2'] == roll_drop[2:]]
        
        sub_avgs_rent['uniq2'] = sub_avgs_rent['metcode'] + sub_avgs_rent['type2']
        sub_avgs_rent = sub_avgs_rent[sub_avgs_rent['uniq2'] == roll_drop]
        sub_avgs_rent.sort_values(by=['subid'], ascending=[True], inplace=True)
        sub_avgs_rent = sub_avgs_rent.drop(['uniq2', 'metcode', 'subid', 'type2', 'count_cat_curr_G_renxAdj_sub'], axis=1)
        sub_avgs_rent = sub_avgs_rent.rename(columns={'uniq1': 'identity', 'avg_cat_curr_G_renxAdj_sub': 'avg_g_monthized', 'stdev_cat_curr_G_renxAdj_sub': 'sd_g_monthized', 
                                    'count_curr_ren_sub_up': 'survs_up', 'count_curr_ren_sub_down': 'survs_down', 'count_curr_ren_sub_zero': 'survs_flat'})

        for col in list(sub_avgs_rent.columns):
            if "_" in col:
                sub_avgs_rent.rename(columns={col: col.replace('_', ' ')}, inplace=True)

        sub_surv_highlighting = get_style(sub_avgs_rent, "partial")

        met_avgs_rent = met_avgs_rent[met_avgs_rent['uniq2'] == roll_drop]
        met_avgs_rent = met_avgs_rent.drop(['avg_cat_G_renxAdj', 'stdev_cat_G_renxAdj', 'count_cat_G_renxAdj', 'count_cat_curr_G_renxAdj_met', 'renx_max_met', 'renx_min_met', 'renx_avg_met', 'renx_sd_met', 'renx_count_met', 'renx_count_uniq_met'], axis=1)
        met_avgs_rent = met_avgs_rent.rename(columns={'uniq2': 'identity', 'avg_cat_curr_G_renxAdj_met': 'avg_g_monthized', 'stdev_cat_curr_G_renxAdj_met': 'sd_g_monthized', 
                                    'count_curr_ren_met_up': 'survs_up', 'count_curr_ren_met_down': 'survs_down', 'count_curr_ren_met_zero': 'survs_flat'})

        for col in list(met_avgs_rent.columns):
            if "_" in col:
                met_avgs_rent.rename(columns={col: col.replace('_', ' ')}, inplace=True)
    
        met_surv_highlighting = get_style(met_avgs_rent, "partial")

        us_avgs_rent = us_avgs_rent.drop(['count_cat_curr_G_renxAdj_us'], axis=1)
        us_avgs_rent = us_avgs_rent.rename(columns={'type2': 'subsector', 'avg_cat_curr_G_renxAdj_us': 'avg_g_monthized', 'stdev_cat_curr_G_renxAdj_us': 'sd_g_monthized', 
                                    'count_curr_ren_us_up': 'survs_up', 'count_curr_ren_us_down': 'survs_down', 'count_curr_ren_us_zero': 'survs_flat'})

        for col in list(us_avgs_rent.columns):
            if "_" in col:
                us_avgs_rent.rename(columns={col: col.replace('_', ' ')}, inplace=True)

        us_surv_highlighting = get_style(us_avgs_rent, "partial")

        sub_rent_surv_title = "Submarket Rent Survey Stats"
        met_rent_surv_title = "Metro Rent Survey Stats"
        us_rent_surv_title = "US Rent Survey Stats"


        msq_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs/{}msq.pickle'.format(sector_val, roll_drop[0:2].lower()))
        if currmon == 1:
            msq_data = msq_data[((msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon) | (msq_data['yr'] == curryr - 1) & (msq_data['currmon'] == 12))]
        else:
            msq_data = msq_data[((msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon) | (msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon - 1))]

        
        msq_data['type2'] = np.where(msq_data['type2'] == "F", "F", "DW")
        msq_data['identity_met'] = msq_data['metcode'] + msq_data['type2']
        msq_data = msq_data[msq_data['type2'] == roll_drop[2:]]
        rent_comps_only_list = ['BD', 'CG', 'DC', 'HL', 'SS']
        msq_data = msq_data[(msq_data['subid'] != 77) | (msq_data['metcode'].isin(rent_comps_only_list))]
        msq_data = msq_data[msq_data['submkt'].str.contains('99') == False]
        msq_data = msq_data[(msq_data['submkt'] != '') & (msq_data['submkt'].isnull() == False)]

        if currmon == 1:
            msq_data = msq_data[((msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon)) | ((msq_data['yr'] == curryr - 1) & (msq_data['currmon'] == 12))]
        else:
            msq_data = msq_data[((msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon)) | ((msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon - 1))]

        msq_data_trunc, sub_list, met_list = set_pool(sector_val, trunc_cols, "trunc_msqs")
        msq_data_trunc['type2'] = np.where(msq_data_trunc['type2'] == "F", "F", "DW")

        if roll_drop[0:2] in expansion_list:
            msq_data_trunc = msq_data_trunc[msq_data_trunc['type2'] == "DW"]
            msq_data_trunc = msq_data_trunc[msq_data_trunc['metcode'].isin(expansion_list)]
        else:
            msq_data_trunc = msq_data_trunc[~msq_data_trunc['metcode'].isin(expansion_list)]
            msq_data_trunc = msq_data_trunc[msq_data_trunc['type2'] == roll_drop[2:]]

        msq_prop_count = msq_data_trunc.copy()
        msq_prop_count = msq_prop_count[(msq_prop_count['yr'] == curryr) & (msq_prop_count['currmon'] == currmon)]
        msq_prop_count['identity'] = msq_prop_count['metcode'] + msq_prop_count['subid'].astype(str) + msq_prop_count['type2']
        msq_prop_count['identity_met'] = msq_prop_count['metcode'] + msq_prop_count['type2']

        for ident in ['identity', 'identity_met', 'type2']:
            temp = msq_prop_count.copy()
            if ident != "type2":
                temp = temp[temp['identity_met'] == roll_drop]
            temp['total_ids'] = temp.groupby(ident)['id'].transform('count')
            temp = temp[temp['renxM'] == 0]
            temp['total_surveyed_ids'] = temp.groupby(ident)['id'].transform('count')
            temp['surv perc'] = temp['total_surveyed_ids'] / temp['total_ids']
            temp = temp.drop_duplicates(ident)
            temp = temp[[ident, 'surv perc']]
            if ident == "identity":
                sub_avgs_rent = sub_avgs_rent.join(temp.set_index(ident), on=ident)
            elif ident == "identity_met":
                temp = temp.rename(columns={ident: 'identity'})
                met_avgs_rent = met_avgs_rent.join(temp.set_index('identity'), on='identity')
            elif ident == "type2":
                temp = temp.rename(columns={ident: 'subsector'})
                us_avgs_rent = us_avgs_rent.join(temp.set_index('subsector'), on='subsector')

        for ident, prefix in zip(['identity', 'identity_met', 'type2'], ['sub_', 'met_', 'us_']):
            if ident != "type2":
                dataframe = msq_data.copy()
            else:
                dataframe = msq_data_trunc.copy()
            dataframe['props_up'] = np.where((dataframe['id'] == dataframe['id'].shift(1)) & (dataframe['renx'] > dataframe['renx'].shift(1)), 1, 0)
            dataframe['props_down'] = np.where((dataframe['id'] == dataframe['id'].shift(1)) & (dataframe['renx'] < dataframe['renx'].shift(1)), 1, 0)
            dataframe['props_flat'] = np.where((dataframe['id'] == dataframe['id'].shift(1)) & (dataframe['renx'] == dataframe['renx'].shift(1)), 1, 0)
            dataframe['props_up'] = dataframe.groupby(ident)['props_up'].transform('sum')
            dataframe['props_down'] = dataframe.groupby(ident)['props_down'].transform('sum')
            dataframe['props_flat'] = dataframe.groupby(ident)['props_flat'].transform('sum')
            
            dataframe[prefix + 'size_all'] = dataframe.groupby([ident, 'currmon'])['ind_size'].transform('sum')
            dataframe['rev'] = dataframe['renx'] * dataframe['ind_size']
            dataframe[prefix + 'rev_all'] = dataframe.groupby([ident, 'currmon'])['rev'].transform('sum')
            dataframe[prefix + 'rent_all'] = dataframe[prefix + 'rev_all'] / dataframe[prefix + 'size_all']
            dataframe[prefix + 'rent_g_all'] = np.where((dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon), (dataframe[prefix + 'rent_all'] - dataframe[prefix + 'rent_all'].shift(1)) / dataframe[prefix + 'rent_all'].shift(1), np.nan)
            dataframe[prefix + 'rent_g_all'] = round(dataframe[prefix + 'rent_g_all'], 3)
            
            dataframe['renxM'] = np.where(dataframe['currmon'] != currmon, dataframe['renxM'].shift(-1), dataframe['renxM'])
            dataframe[prefix + 'size_surv'] = dataframe[(dataframe['renxM'] == 0)].groupby([ident, 'currmon'])['ind_size'].transform('sum')
            dataframe[prefix + 'rev_surv'] = dataframe[(dataframe['renxM'] == 0)].groupby([ident, 'currmon'])['rev'].transform('sum')
            dataframe[prefix + 'rev_surv'] = dataframe.groupby([ident, 'currmon'])[prefix + 'rev_surv'].bfill()
            dataframe[prefix + 'rev_surv'] = dataframe.groupby([ident, 'currmon'])[prefix + 'rev_surv'].ffill()
            dataframe[prefix + 'size_surv'] = dataframe.groupby([ident, 'currmon'])[prefix + 'size_surv'].bfill()
            dataframe[prefix + 'size_surv'] = dataframe.groupby([ident, 'currmon'])[prefix + 'size_surv'].ffill()
            dataframe[prefix + 'rent_surv'] = dataframe[prefix + 'rev_surv'] / dataframe[prefix + 'size_surv']
            dataframe[prefix + 'rent_g_surv'] = np.where((dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon), (dataframe[prefix + 'rent_surv'] - dataframe[prefix + 'rent_surv'].shift(1)) / dataframe[prefix + 'rent_surv'].shift(1), np.nan)
            dataframe[prefix + 'rent_g_surv'] = round(dataframe[prefix + 'rent_g_surv'], 3)
            
            dataframe[prefix + 'size_sq'] = dataframe[(dataframe['renxM'] == 1)].groupby([ident, 'currmon'])['ind_size'].transform('sum')
            dataframe[prefix + 'rev_sq'] = dataframe[(dataframe['renxM'] == 1)].groupby([ident, 'currmon'])['rev'].transform('sum')
            dataframe[prefix + 'rev_sq'] = dataframe.groupby([ident, 'currmon'])[prefix + 'rev_sq'].bfill()
            dataframe[prefix + 'rev_sq'] = dataframe.groupby([ident, 'currmon'])[prefix + 'rev_sq'].ffill()
            dataframe[prefix + 'size_sq'] = dataframe.groupby([ident, 'currmon'])[prefix + 'size_sq'].bfill()
            dataframe[prefix + 'size_sq'] = dataframe.groupby([ident, 'currmon'])[prefix + 'size_sq'].ffill()
            dataframe[prefix + 'rent_sq'] = dataframe[prefix + 'rev_sq'] / dataframe[prefix + 'size_sq']
            dataframe[prefix + 'rent_g_sq'] = np.where((dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon), (dataframe[prefix + 'rent_sq'] - dataframe[prefix + 'rent_sq'].shift(1)) / dataframe[prefix + 'rent_sq'].shift(1), np.nan)
            dataframe[prefix + 'rent_g_sq'] = round(dataframe[prefix + 'rent_g_sq'], 3)

            dataframe = dataframe[(dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon)]
            dataframe = dataframe.drop_duplicates(ident)
            if ident == "identity":
                msq_data_sub_rent = dataframe.copy()
            elif ident == "identity_met":
                msq_data_met_rent = dataframe.copy()
            elif ident == "type2":
                msq_data_us_rent = dataframe.copy()
        
        msq_data_sub_rent.sort_values(by=['subid'], ascending=[True], inplace=True)
        msq_data_sub_rent = msq_data_sub_rent[['identity', 'sub_rent_g_surv', 'sub_rent_g_sq', 'sub_rent_g_all', 'props_up', 'props_down', 'props_flat']]
        msq_data_met_rent = msq_data_met_rent[['identity_met', 'met_rent_g_surv', 'met_rent_g_sq', 'met_rent_g_all', 'props_up', 'props_down', 'props_flat']]
        msq_data_met_rent = msq_data_met_rent.rename(columns={'identity_met': 'identity'})
        msq_data_us_rent = msq_data_us_rent[['type2', 'us_rent_g_surv', 'us_rent_g_sq', 'us_rent_g_all', 'props_up', 'props_down', 'props_flat']]
        msq_data_us_rent = msq_data_us_rent.rename(columns={'type2': 'subsector'})
        
        for col in list(msq_data_sub_rent.columns):
            if "_" in col:
                msq_data_sub_rent.rename(columns={col: col.replace('_', ' ')}, inplace=True)
        for col in list(msq_data_met_rent.columns):
            if "_" in col:
                msq_data_met_rent.rename(columns={col: col.replace('_', ' ')}, inplace=True)
        for col in list(msq_data_us_rent.columns):
            if "_" in col:
                msq_data_us_rent.rename(columns={col: col.replace('_', ' ')}, inplace=True)

        sub_sq_highlighting = get_style(msq_data_sub_rent, 'partial')
        met_sq_highlighting = get_style(msq_data_met_rent, 'partial')
        us_sq_highlighting = get_style(msq_data_us_rent, 'partial')

        type_dict, format_dict = get_types("ind")

        sub_rent_sq_title = "Current SQ Submarket Rent Roll"
        met_rent_sq_title = "Current SQ Metro Rent Roll"
        if roll_drop[0:2] in expansion_list:
            tier = "Expansion"
        else:
            tier = "Legacy"
        us_rent_sq_title = "Current SQ {} US Rent Roll".format(tier)

        sub_avgs_rent[['survs up', 'survs down', 'survs flat', 'surv perc']] = sub_avgs_rent[['survs up', 'survs down', 'survs flat', 'surv perc']].fillna(0)
        met_avgs_rent[['survs up', 'survs down', 'survs flat', 'surv perc']] = met_avgs_rent[['survs up', 'survs down', 'survs flat', 'surv perc']].fillna(0)
        us_avgs_rent[['survs up', 'survs down', 'survs flat', 'surv perc']] = us_avgs_rent[['survs up', 'survs down', 'survs flat', 'surv perc']].fillna(0)
        msq_data_sub_rent[['props up', 'props down', 'props flat']] = msq_data_sub_rent[['props up', 'props down', 'props flat']].fillna(0)
        msq_data_met_rent[['props up', 'props down', 'props flat']] = msq_data_met_rent[['props up', 'props down', 'props flat']].fillna(0)
        msq_data_us_rent[['props up', 'props down', 'props flat']] = msq_data_us_rent[['props up', 'props down', 'props flat']].fillna(0)

        return False, sub_avgs_rent.to_dict('records'), [{'name': [sub_rent_surv_title, sub_avgs_rent.columns[i]], 'id': sub_avgs_rent.columns[i], 'type': type_dict[sub_avgs_rent.columns[i]], 'format': format_dict[sub_avgs_rent.columns[i]]}
                                 for i in range(0, len(sub_avgs_rent.columns))], sub_surv_highlighting, met_avgs_rent.to_dict('records'), [{'name': [met_rent_surv_title, met_avgs_rent.columns[i]], 'id': met_avgs_rent.columns[i], 'type': type_dict[met_avgs_rent.columns[i]], 'format': format_dict[met_avgs_rent.columns[i]]}
                                 for i in range(0, len(met_avgs_rent.columns))], met_surv_highlighting, us_avgs_rent.to_dict('records'), [{'name': [us_rent_surv_title, us_avgs_rent.columns[i]], 'id': us_avgs_rent.columns[i], 'type': type_dict[us_avgs_rent.columns[i]], 'format': format_dict[us_avgs_rent.columns[i]]}
                                 for i in range(0, len(us_avgs_rent.columns))], us_surv_highlighting, msq_data_sub_rent.to_dict('records'), [{'name': [sub_rent_sq_title, msq_data_sub_rent.columns[i]], 'id': msq_data_sub_rent.columns[i], 'type': type_dict[msq_data_sub_rent.columns[i]], 'format': format_dict[msq_data_sub_rent.columns[i]]}
                                 for i in range(0, len(msq_data_sub_rent.columns))], sub_sq_highlighting, msq_data_met_rent.to_dict('records'), [{'name': [met_rent_sq_title, msq_data_met_rent.columns[i]], 'id': msq_data_met_rent.columns[i], 'type': type_dict[msq_data_met_rent.columns[i]], 'format': format_dict[msq_data_met_rent.columns[i]]}
                                 for i in range(0, len(msq_data_met_rent.columns))], met_sq_highlighting, msq_data_us_rent.to_dict('records'), [{'name': [us_rent_sq_title, msq_data_us_rent.columns[i]], 'id': msq_data_us_rent.columns[i], 'type': type_dict[msq_data_us_rent.columns[i]], 'format': format_dict[msq_data_us_rent.columns[i]]}
                                 for i in range(0, len(msq_data_us_rent.columns))], us_sq_highlighting

# Load MSQs Callbacks

@sq_support.callback(Output('confirm_msq_pickle', 'displayed'),
                     [Input('sector', 'data')],
                     [State('curryr', 'data'),
                     State('currmon', 'data'),
                     State('program', 'data')])
def confirm_msq_pickle(sector_val, curryr, currmon, program):
    if program != "load_msqs":
        raise PreventUpdate
    else:
        root = Path("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs".format(sector_val))
        file_list = [f for f in listdir(root) if isfile(join(root, f))]
        if len(file_list) == 0:
            return True
        else:
            test = pd.read_pickle("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs/{}".format(sector_val, file_list[0]))
            test = test[(test['yr'] == curryr) & (test['currmon'] == currmon)]
            if len(test) > 0:
                return True
            else:
                return False


@sq_support.callback([Output('msq_load_alert', 'is_open'),
                     Output('msq_load_alert_text', 'children'),
                     Output('msq_load_alert', 'color'),
                     Output('loading-msqs', 'children')],
                     [Input('confirm_msq_pickle', 'submit_n_clicks')],
                     [State('curryr', 'data'),
                     State('currmon', 'data'),
                     State('program', 'data'),
                     State('sector', 'data')])

def pickle_msqs(confirmed, curryr, currmon, program, sector_val):
    if program != "load_msqs":
        raise PreventUpdate
    else:
        message, color = convert_msqs(sector_val, curryr, currmon)

        return True, message, color, True

@sq_support.callback(Output('load_msq_home_url','pathname'),
                    [Input('msq_load_alert', 'is_open'),
                    Input('confirm_msq_pickle', 'cancel_n_clicks'),
                    Input('confirm_msq_pickle', 'submit_n_clicks')])

def load_msqs_return_to_login(alert_open, not_confirmed, confirmed):
    if not_confirmed is None and confirmed is None:
        raise PreventUpdate
    elif alert_open is None or alert_open==True:
        return no_update
    else:
        session['authed'] = False
        return '/login'

# Load Logs Callbacks

@sq_support.callback([Output('log_load_alert', 'is_open'),
                     Output('log_load_alert_text', 'children'),
                     Output('log_load_alert', 'color'),
                     Output('loading-logs', 'children'),
                     Output('store_log_errors', 'data')],
                     [Input('sector', 'data')],
                     [State('curryr', 'data'),
                     State('currmon', 'data'),
                     State('program', 'data')])

def load_logs(sector_val, curryr, currmon, program):
    if program != "load_logs":
        raise PreventUpdate
    else:
        message, color, error_list = load_log_files(sector_val, curryr, currmon)
        return True, message, color, True, error_list

@sq_support.callback([Output('log_errors_alert', 'is_open'),
                     Output('log_errors_alert_text', 'children'),
                     Output('log_errors_alert', 'color')],
                     [Input('store_log_errors', 'data')])

def display_log_errors(log_errors):
    if log_errors is None:
        raise PreventUpdate
    else:
        if len(log_errors) == 0:
            display = False
        else:
            display = True
            text = "The following IDs have a comma in a key field, and therefore could not be loaded. Remove the comma in Foundation so the property can be loaded in next month: \n\n" + ', '.join(map(str, log_errors))
        return display, text, 'danger'

@sq_support.callback(Output('load_logs_home_url','pathname'),
                    [Input('log_load_alert', 'is_open')])

def load_logs_return_to_login(alert_open):
    if alert_open is None or alert_open==True:
        return no_update
    session['authed'] = False
    return '/login'

# Survey Benchmark Callbacks

@sq_support.callback([Output('benchmarks_load_alert', 'is_open'),
                     Output('benchmarks_load_alert_text', 'children'),
                     Output('benchmarks_load_alert', 'color'),
                     Output('loading-benchmarks', 'children')],
                     [Input('sector', 'data')],
                     [State('curryr', 'data'),
                     State('currmon', 'data'),
                     State('program', 'data')])

def load_benchmarks(sector_val, curryr, currmon, program):
    if program != "surv_bench":
        raise PreventUpdate
    else:
        process_survey_benchmarks(sector_val, curryr, currmon)
        return True, "Survey Benchmark Input Files Processed and Saved. Click the x to return to the program selection screen", "success", True

@sq_support.callback(Output('benchmarks_home_url','pathname'),
                    [Input('benchmarks_load_alert', 'is_open')])

def load_benchmarks_return_to_login(alert_open):
    if alert_open is None or alert_open==True:
        return no_update
    session['authed'] = False
    return '/login'

# Exam Survey Callbacks

@sq_support.callback([Output('loading-survs', 'children'),
                      Output('surv_table', 'data'),
                      Output('surv_table', 'columns'),
                      Output('surv_table', 'style_data_conditional'),
                      Output('surv_table', 'style_cell_conditional'),
                      Output('struct_cols', 'data'),
                      Output('first_trigger', 'data')],
                      [Input('sector', 'data')],
                      [State('curryr', 'data'),
                      State('currmon', 'data'),
                      State('program', 'data'),
                      State('first_trigger', 'data')])

def load_survs(sector_val, curryr, currmon, program, first_trigger):
    if program != "exam_survs":
        raise PreventUpdate
    else:

        if not first_trigger:
            first_trigger = True
        else:
            first_trigger = no_update

        df, struct_cols = search_logs_load(sector_val, curryr, currmon)

        df = df[[x for x in df.columns if x not in struct_cols]]

        for col in df.columns:
            df.rename(columns={col: col.replace('_', ' ')}, inplace=True)

        highlighting = get_style(df, 'partial')

        type_dict, format_dict = get_types(sector_val)

        cell_widths = []
        width = int(math.floor(100 / len(df.columns)))
        for col in df.columns:
            cell_widths.append({'if': {'column_id': col}, 'width': '{}%'.format(width)})

        return True, df.to_dict('records'), [{'name': ['Surveys', df.columns[i]], 'id': df.columns[i], 'type': type_dict[df.columns[i]], 'format': format_dict[df.columns[i]]}
                                 for i in range(0, len(df.columns))], highlighting, cell_widths, struct_cols, first_trigger

@sq_support.callback([Output('secondary_table', 'data'),
                     Output('secondary_table', 'columns')],
                     [Input('surv_table', 'filter_query'),
                      Input('first_trigger', 'data')],
                      [State('sector', 'data'),
                      State('program', 'data'),
                      State('struct_cols', 'data')])

def gen_structural_table(filter_in, trigger, sector_val, program, struct_cols):

    if program != "exam_survs":
        raise PreventUpdate
    else:

        df = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/intermediatefiles/{}_logs.pickle'.format(sector_val))

        if filter_in == None:
            df = df.head(1)
        else:
            try:
                filt_col = filter_in.split('=')[0].strip().replace('{', '').replace('}', '')
                id_filt = filter_in.split('=')[-1].strip()
                if filt_col == 'realid':
                    df = df[df[filt_col] == id_filt].head(1)
                else:
                    df = pd.DataFrame()
            except:
                df = pd.DataFrame()
            
        if len(df) > 0:
            df = df[struct_cols]
        for col in df.columns:
            df.rename(columns={col: col.replace('_', ' ')}, inplace=True)

        type_dict, format_dict = get_types(sector_val)

        return df.to_dict('records'), [{'name': ['Structurals', df.columns[i]], 'id': df.columns[i], 'type': type_dict[df.columns[i]], 'format': format_dict[df.columns[i]]}
                                 for i in range(0, len(df.columns))]

@sq_support.callback(Output('exam_survs_url','pathname'),
                     [Input('logout-button','n_clicks')])
def exam_survs_logout(n_clicks):
    input_id = get_input_id()
    if (n_clicks is None or n_clicks==0):
        return no_update
    
    session['authed'] = False
    return '/login'

def load_benchmarks_return_to_login(alert_open):
    if alert_open is None or alert_open==True:
        return no_update
    session['authed'] = False
    return '/login'


# Flag Program Callbacks

@sq_support.callback([Output('flags_processed', 'data'),
                     Output('flag_file_name', 'data'),
                     Output('flag_cols', 'data'),
                     Output('msq_sub_drop', 'options'),
                     Output('msq_sub_drop_container', 'style'),
                     Output('roll_drop', 'options'),
                     Output('roll_drop', 'value'),
                     Output('roll_drop_container', 'style')],
                     [Input('sector', 'data')],
                     [State('curryr', 'data'),
                     State('currmon', 'data'),
                     State('program', 'data'),
                     State('trunc_cols', 'data')])

def process_flags(sector_val, curryr, currmon, program, trunc_cols):
    if sector_val is None:
        raise PreventUpdate
    else:

        msq_data, sub_list, met_list = set_pool(sector_val, trunc_cols, "curr_msqs")
            
        id_identity_list = msq_data.copy()
        id_identity_list = id_identity_list.drop_duplicates('id')
        id_identity_list['type2'] = np.where((id_identity_list['type2'] == "F"), "F", "DW")
        id_identity_list['identity'] = id_identity_list['metcode'] + id_identity_list['subid'].astype(str) + id_identity_list['type2']
        id_identity_list = id_identity_list[['id', 'identity']]
        id_identity_list.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/id_identity_list.pickle'.format(sector_val, curryr, currmon))
        
        if program == "sq_capture":
            flag_cols = sq_capture_survey_flags(sector_val, curryr, currmon, msq_data)
            flag_file_name = 'surv_check_flags'
        elif program == "sq_logic":
            flag_cols = sq_logic_flags(sector_val, curryr, currmon, msq_data, False)
            flag_file_name = 'sq_logic'
        elif program == "outlier_level":
            past_msq_data, sub_list, met_list = set_pool(sector_val, trunc_cols, "prior_msqs")
            flag_cols = outlier_r_level_flags(sector_val, curryr, currmon, msq_data, past_msq_data)
            flag_file_name = "outlier_r_level"
        elif program == "surv_chgs":
            flag_cols = get_surv_changes(sector_val, curryr, currmon)
            flag_file_name = "surv_chgs"
        elif program == "out_sublet":
            flag_cols = sublet_flags(sector_val, curryr, currmon)
            flag_file_name = 'sublet_check_flags'
        elif program == "ren_g":
            g_dist_flags(sector_val, curryr, currmon, msq_data, program)
            flag_cols = ren_g_flags(sector_val, curryr, currmon, msq_data)
            flag_file_name = 'ren_g'
        elif program == "vac_g":
            flag_cols = vac_g_flags(sector_val, curryr, currmon, msq_data)
            flag_file_name = 'vac_g'
        elif program == "or_rel":
            flag_cols = opex_rent_flags(sector_val, curryr, currmon, msq_data)
            flag_file_name = 'or_flags'
        elif program == "tax_opex":
            flag_cols = tax_opex_flags(sector_val, curryr, currmon, msq_data)
            flag_file_name = "to_flags"
        elif program == 'sq_hist':
            past_msq_data, sub_list, met_list = set_pool(sector_val, trunc_cols, "prior_msqs")
            flag_cols = sq_hist_flags(sector_val, curryr, currmon, msq_data, past_msq_data)
            flag_file_name = 'sq_hist_flags'

        sub_drop_style = {'display': 'inline-block', 'width': '12%'}

        roll_drop_style = {'display': 'block', 'width': '18%', 'padding-left': '30px'}
        
        return True, flag_file_name, flag_cols, [{'label': i, 'value': i} for i in sub_list], sub_drop_style, [{'label': i, 'value': i} for i in met_list], met_list[0], roll_drop_style

@sq_support.callback(Output('flags_home_url','pathname'),
                     [Input('logout-button','n_clicks'),
                     Input('no_flag_alert', 'is_open')])
def flag_program_logout(n_clicks, no_flag):
    '''clear the session and send user to login'''
    input_id = get_input_id()
    if (n_clicks is None or n_clicks==0) and input_id != 'no_flag_alert':
        return no_update
    elif no_flag is None or no_flag == True:
        raise PreventUpdate
    
    session['authed'] = False
    return '/login'

server_check = os.getcwd()
    
if server_check[0:6] == "\\Odin":
    server = 0
else:
    server = 1


if __name__ == '__main__':
    
    if server == 1:
        test_ports = [8070, 8060, 8040, 8030, 8000]
        for x in test_ports:
            try:
                print("Trying port %d" % (x))
                sq_support.run_server(port=x, host='0.0.0.0')
                break
            except:
                print("Port being used, trying another")
    elif server == 0:
        sq_support.run_server(debug=True)



