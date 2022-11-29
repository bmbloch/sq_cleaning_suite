# Use this program to find ids that are missing a square observation for renx, opex, taxx, availx, and totavailx or that have an illogical observation


import csv
import pandas as pd
import numpy as np
from pathlib import Path
import os
import re
import time
from IPython.core.display import display
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)

def sq_logic_flags(sector_val, curryr, currmon, msq_data_in, finalizer):

    flag_cols = []
    data_test = msq_data_in.copy()
    data_test = data_test.rename(columns={'ind_size': 'sizex'})
    for x in ['renx', 'opex', 'taxx', 'availx', 'totavailx']:
        data_test['missing_' + x] = np.where((np.isnan(data_test[x]) == True), 1, 0)
        flag_cols.append('missing_' + x)

    for x in ['renx', 'opex', 'taxx', 'availx', 'totavailx']:
        data_test['negative_' + x] = np.where(data_test[x] < 0, 1, 0)
        flag_cols.append('negative_' + x)

    for x in ['renx', 'opex', 'taxx']:
        data_test['zero_' + x] = np.where(data_test[x] == 0, 1, 0)
        flag_cols.append('zero_' + x)

    if finalizer == True:
        threshold_hi = 1
        threshold_low = 0
    elif finalizer == False:
        threshold_hi = 0.9
        threshold_low = 0.05

    data_test['opex_rent'] = np.where((data_test['opex'] / (data_test['opex'] + data_test['renx']) >= threshold_hi) | (data_test['opex'] / (data_test['opex'] + data_test['renx']) < threshold_low), 1, 0)
    flag_cols.append('opex_rent')

    if finalizer == True:
        threshold = 1
    elif finalizer == False:
        threshold = 0.95
    
    data_test['opex_tax'] = np.where(data_test['taxx'] / data_test['opex'] >= threshold, 1, 0)
    flag_cols.append('opex_tax')

    for x in ['vacratx', 'totvacx']:
        data_test['above_' + x] = np.where(data_test[x] > 1, 1, 0)
        flag_cols.append('above_' + x)

    for x in ['availx', 'totavailx']:
        data_test['size_' + x] = np.where(data_test[x] > data_test['sizex'], 1, 0)
        flag_cols.append('size_' + x)

    data_flags = data_test.copy()
    data_flags['has_flag'] = data_flags[flag_cols].any(1)
    data_flags = data_flags[data_flags['has_flag'] == True]

    data_flags['type2'] = np.where(data_flags['type2'] == "F", "F", "DW")
    data_flags['identity'] = data_flags['metcode'] + data_flags['subid'].astype(str) + data_flags['type2']
    data_flags = data_flags[['identity', 'id', 'realid', 'metcode', 'yr', 'qtr', 'currmon', 'survdate', 'renx', 'opex', 'taxx', 'availx', 'totavailx', 'vacratx', 'totvacx', 'sizex'] + flag_cols]

    
    for x in flag_cols:
        if "renx" in x or "opex" in x or "taxx" in x:
            data_flags.rename(columns={x: 'r_flag_' + x}, inplace=True)
        else:
            data_flags.rename(columns={x: 'v_flag_' + x}, inplace=True)
    flag_cols = ["r_flag_" + x if ("renx" in x or "opex" in x or "taxx" in x) else "v_flag_" + x for x in flag_cols]

    data_flags['qtr'] = data_flags['qtr'].astype(int)
    data_flags['currmon'] = data_flags['currmon'].astype(int)
    
    data_flags['flag_period'] = np.where(data_flags['currmon'].isnull() == True, data_flags['qtr'].astype(str) + "/" + data_flags['yr'].astype(str), data_flags['currmon'].astype(str) + "/" + data_flags['qtr'].astype(str) + "/" + data_flags['yr'].astype(str))

    data_flags.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sq_logic.pickle'.format(sector_val, curryr, currmon))

    return flag_cols

