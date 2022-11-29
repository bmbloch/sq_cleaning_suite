import csv
import pandas as pd
import numpy as np
import sys
import random
import itertools
import math
import time
from datetime import datetime
from pathlib import Path
import re
import os
from os import listdir
from os.path import isfile, join
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.2f}'.format
from IPython.core.display import display, HTML
from tqdm import tqdm

def get_home():
    if os.name == "nt": return "//odin/reisadmin/"
    else: return "/home/"


def search_logs_load(sector_val, curryr, currmon):

    df = pd.DataFrame()

    root = '{}central/square/data/{}/download/test2022/'.format(get_home(), sector_val)

    dir_list = [f for f in listdir(root) if isfile(join(root, f))]

    file_list = [x for x in dir_list if len(x.split('/')[-1].split('.log')[0]) == 2]

    for path in tqdm(file_list):
        
        file_read = root + path

        load_dict = {'apt': {'cols': {'id': 'str',
                                      'metcode': 'str', 
                                      'subid': 'float', 
                                      'totunits': 'float', 
                                      'ren0': 'float', 
                                      'ren1': 'float', 
                                      'ren2': 'float', 
                                      'ren3': 'float',
                                      'ren4': 'float',
                                      'avgren': 'float',
                                      'vac0': 'float', 
                                      'vac1': 'float', 
                                      'vac2': 'float', 
                                      'vac3': 'float',
                                      'vac4': 'float',
                                      'avail': 'float',
                                      'property_source_id': 'str'},
                              'struct_cols': {
                                                'propname': 'str',
                                                'address': 'str',
                                                'city': 'str',
                                                'zip': 'str',
                                                'county': 'str',
                                                'state': 'str',
                                                'year': 'float',
                                                'month': 'float',
                                                'flrs': 'float',
                                                'x': 'float',
                                                'y': 'float',
                                                'units0': 'float',
                                                'units1': 'float',
                                                'units2': 'float',
                                                'units3': 'float',
                                                'units4': 'float',
                                                'estunit': 'str' 
                                             },
                              'date_cols': ['survdate']},
                     'off': {'cols': {'realid': 'str',
                                      'type2': 'str',
                                      'metcode': 'str', 
                                      'subid': 'float', 
                                      'size': 'float', 
                                      'avail': 'float', 
                                      'sublet': 'float', 
                                      'avrent': 'float', 
                                      'op_exp': 'float', 
                                      'rnt_term': 'str', 
                                      'source': 'str',
                                      'property_source_id': 'str'},
                             'struct_cols': {
                                             'propname': 'str',
                                             'address': 'str',
                                             'city': 'str',
                                             'zip': 'str',
                                             'county': 'str',
                                             'state': 'str',
                                             'year': 'float',
                                             'month': 'float',
                                             'bldgs': 'float',
                                             'x': 'float',
                                             'y': 'float',
                                            },
                             'date_cols': ['survdate']},
                     'ind': {'cols': {'realid': 'str',
                                      'type2': 'str',
                                      'metcode': 'str', 
                                      'subid': 'float', 
                                      'ind_size': 'float', 
                                      'ind_avail': 'float', 
                                      'sublet': 'float', 
                                      'ind_avrent': 'float', 
                                      'op_exp': 'float', 
                                      'rnt_term': 'str', 
                                      'source': 'str',
                                      'property_source_id': 'str'},
                                'struct_cols': {
                                                'propname': 'str',
                                                'address': 'str',
                                                'city': 'str',
                                                'zip': 'str',
                                                'county': 'str',
                                                'state': 'str',
                                                'year': 'float',
                                                'month': 'float',
                                                'bldgs': 'float',
                                                'x': 'float',
                                                'y': 'float',
                                                'docks': 'float',
                                                'dockhigh_doors': 'float',
                                                'drivein_doors': 'float',
                                                'rail_doors': 'float',
                                                'ceil_avg': 'float'
                                                },
                                'date_cols': ['survdate']},
                    'ret':  {'cols': {'id': 'str',
                                      'type1': 'str',
                                      'msa': 'str', 
                                      'gsub': 'float', 
                                      'n_size': 'float', 
                                      'a_size': 'float',
                                      'tot_size': 'float', 
                                      'n_avail': 'float',
                                      'a_avail': 'float', 
                                      'n_avrent': 'float',
                                      'a_avrent': 'float', 
                                      'expoper': 'float', 
                                      'rent_basis': 'str', 
                                      'source': 'str',
                                      'property_source_id': 'str'},
                            'struct_cols': {
                                            'name': 'str',
                                            'addr': 'str',
                                            'city': 'str',
                                            'zip': 'str',
                                            'county': 'str',
                                            'st': 'str',
                                            'first_year': 'float',
                                            'month': 'float',
                                            'x_long': 'float',
                                            'y_lat': 'float'
                                          },
                            'date_cols': ['surv_date'],
                            }
                    }
        
        use_cols = list(load_dict[sector_val]['cols'].keys()) + load_dict[sector_val]['date_cols'] + list(load_dict[sector_val]['struct_cols'])
        dtypes = {**load_dict[sector_val]['cols'], **load_dict[sector_val]['struct_cols']}

        temp = pd.read_csv(file_read, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False, usecols=use_cols, dtype=dtypes, parse_dates=load_dict[sector_val]['date_cols'])
        if len(temp.columns) == 1:
            temp = pd.read_csv(file_read, sep='\t', encoding = 'utf-8',  na_values= "", keep_default_na = False, usecols=use_cols, dtype=dtypes, parse_dates=load_dict[sector_val]['date_cols'])

        df = df.append(temp, ignore_index=True)

    df.columns= df.columns.str.lower()
    
    consistency_dict = {'apt': {
                                'id': 'realid'
                               },
                        'ret': {
                                'id': 'realid',
                                'surv_date': 'survdate',
                                'msa': 'metcode',
                                'gsub': 'subid',
                                'x_long': 'x',
                                'y_lat': 'y',
                                'first_year': 'year',
                                'renov_year': 'renov',
                                'name': 'propname',
                                'addr': 'address',
                                'st': 'state',
                                'expoper': 'op_exp',
                                'rent_basis': 'rnt_term',
                                }
                       }
    
    if sector_val in list(consistency_dict.keys()):
        for key, value in consistency_dict[sector_val].items():
            if key in df.columns:
                df.rename(columns={key: value}, inplace=True)
    
    df['survdate'] = pd.to_datetime(df['survdate']).dt.date
    df.sort_values(by=['realid', 'survdate'], ascending=[True, False], inplace=True)

    df['zip'] = np.where((df['zip'].str.contains('.') == True), df['zip'].str.split('.').str[0].str.strip(), df['zip'])
    df['zip'] = np.where((df['zip'].str.contains('-') == True), df['zip'].str.split('-').str[0].str.strip(), df['zip'])
    df['zip'] = np.where((df['zip'].str.isdigit() == False), np.nan, df['zip'])
    df['zip'] = df['zip'].astype(float)

    df.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/intermediatefiles/{}_logs.pickle'.format(sector_val))

    struct_cols = list(load_dict[sector_val]['struct_cols'].keys())
    if sector_val in list(consistency_dict.keys()):
        for key, value in consistency_dict[sector_val].items():
            if key in struct_cols:
                struct_cols = [x if x != key else value for x in struct_cols]

    return df, struct_cols