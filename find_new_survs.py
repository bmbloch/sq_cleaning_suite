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
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.2f}'.format
from IPython.core.display import display, HTML

def get_surv_changes(sector_val, curryr, currmon):

    if currmon == 1:
        pastyr = curryr - 1
        pastmon = 12
    else:
        pastyr = curryr
        pastmon = currmon - 1

    data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))

    data = data[(data['reisyr'] < curryr) | ((data['reisyr'] == curryr) & (data['reismon'] <= currmon))]

    # Set the path to the past months logs input file
    directory_in_str = '/home/central/square/data/ind/download/{}m{}'.format(pastyr, pastmon)
    pathlist = Path(directory_in_str).glob('**/*.log')

    # List of Published Metros
    pub_list = ["aa", "ab", "ak", "an", "aq", "at", "au", "ba", "bd", 
    "bf", "bi", "bo", "br", "bs", "cg", "ch", "ci", "cj", "cl", "cm",
    "cn", "co", "cr", "cs", "da", "dc", "de", "dm", "dn", "dt", "ep",
    "fc", "fl", "fm", "fr", "fw", "gd", "gn", "gr", "hl", "ho", "hr",
    "ht", "in", "ja", "kc", "kx", "la", "li", "ll", "lo", "lr", "lv",
    "lx", "me", "mi", "mn", "mw", "na", "nf", "nj", "nm", "no", "ny",
    "oa", "oc", "ok", "om", "or", "pa", "pb", "pi", "po", "pv", "px",
    "rd", "re", "ri", "ro", "sa", "sb", "sc", "sd", "se", "sf", "sh",
    "sj", "sl", "sm", "so", "sr", "ss", "st", "sv", "sy", "ta", "tc",
    "tm", "to", "tu", "vj", "vn", "wc", "wk", "wl", "ws"]

    # Get the data from the archived download log files for the past period, and load it all into a dataframe
    count_files = 0
    count_met = 0
    df_cols = []
    data_list = []
    error_list = []
    for path in pathlist:
        count_cols = 0
        path_in_str = str(path)
        with open(path_in_str, "r") as file:
            test = path_in_str[-6:-4]
        if test in pub_list:
            count_files += 1
            with open(path_in_str, "r") as file:
                for line in file:
                    if "\t" in line:
                        line_split = line.split("\t")
                    else:
                        line_split = line.split(",")
                    if count_cols == 0:
                        df_cols = line_split
                        df_cols = [x.lower() for x in df_cols]
                        if count_met == 0:
                            data_past = pd.DataFrame(columns = df_cols)
                            count_met = 1
                        count_cols = 1
                    else:
                        if len(line_split) == len(df_cols):
                            data_list.append(line_split)
                        else:
                            error_list.append(line_split)
        if count_files == len(pub_list):
            break

    data_past = data_past.append(pd.DataFrame(data_list, columns = df_cols))

    # Strip out the quotations from each data point
    for i, col in enumerate(data_past.columns):
        data_past.iloc[:, i] = data_past.iloc[:, i].str.replace('"', '')

    data_past[['survmon','survday', 'survyr']] = data_past['survdate'].str.split("/",expand=True)
    data_past[['survmon', 'survday', 'survyr']] = data_past[['survmon', 'survday', 'survyr']].astype(int)
    data_past['reismon'] = np.where((data_past['survday'] <= 15), data_past['survmon'], data_past['survmon'] + 1)
    data_past['reismon'] = np.where((data_past['survday'] > 15) & (data_past['survmon'] == 12), 1, data_past['reismon'])
    data_past['reisyr'] = np.where((data_past['survmon'] != 12), data_past['survyr'], data_past['survyr'] + 1)
    data_past['reisyr'] = np.where((data_past['survmon'] == 12) & (data_past['survday'] <= 15), data_past['survyr'], data_past['reisyr'])

    data_past['survdate']= pd.to_datetime(data_past['survdate'])

    for col in list(data_past.columns):
        if col != "survdate":
            data_past[col] = np.where(data_past[col] == '', np.nan, data_past[col])

    data_past['sublet'] = data_past['sublet'].astype(float)
    data_past['ind_avail'] = data_past['ind_avail'].astype(float)
    data_past['ind_size'] = data_past['ind_size'].astype(float)
    data_past['ind_avrent'] = data_past['ind_avrent'].astype(float)
    data_past['op_exp'] = data_past['op_exp'].astype(float)
    data_past['re_tax'] = data_past['re_tax'].astype(float)
    data_past['realid'] = data_past['realid'].astype(int)
    data_past['year'] = data_past['year'].astype(float)
    data_past['month'] = data_past['month'].astype(float)
    data_past['subid'] = data_past['subid'].astype(float)


    data = data[data['reisyr'] >= curryr - 6]

    data = data[data['subid'].isnull() == False]

    data['currtag'] = np.where((data['reisyr'] == curryr) & (data['reismon'] == currmon), 1, 0)

    data['identity_period'] = data['realid'].astype(str) + data['survdate'].astype(str)
    data_past['identity_period'] = data_past['realid'].astype(str) + data_past['survdate'].astype(str)

    for col in ['ind_avrent', 'op_exp', 're_tax', 'ind_avail']:
        data[col] = np.where(data[col] == '', 999999999, data[col])
        data_past[col] = np.where(data_past[col] == '', 999999999, data_past[col])
        data[col] = data[col].astype(float)
        data_past[col] = data_past[col].astype(float)
        data[col] = np.where(data[col] == 999999999, np.nan, data[col])
        data_past[col] = np.where(data_past[col] == 999999999, np.nan, data_past[col])

    for sort_order, col_name, var_name in zip([False, True, False, True, False, True], ['p_avrent_first', 'p_avrent_second', 'p_opex_first', 'p_opex_second', 'p_retax_first', 'p_retax_second', 'p_avail_first', 'p_avail_second'], ['ind_avrent', 'ind_avrent', 'op_exp', 'op_exp', 're_tax', 're_tax', 'ind_avail', 'ind_avail']):
        temp = data_past.copy()
        temp = temp[np.isnan(temp[var_name]) == False]
        if "second" in col_name:
            temp['count_same_day_survs'] = temp.groupby(['realid', 'survdate'])['realid'].transform('count')
            temp = temp[temp['count_same_day_survs'] > 1]
        temp.sort_values(by=['realid', 'survdate', var_name], ascending=[True, True, sort_order], inplace=True)
        temp = temp.drop_duplicates(['realid', 'survdate'])
        temp = temp[['identity_period', var_name]]
        temp = temp.set_index('identity_period')
        temp.columns = [col_name]
        data = data.join(temp, on='identity_period')
    
    data['count_same_day_survs'] = data.groupby(['realid', 'survdate'])['realid'].transform('count')
    data['count_same_day_rent'] = data.groupby(['realid', 'survdate'])['ind_avrent'].transform('count')

    data['new_rent'] = np.where((data['ind_avrent'].isnull() == False) & (data['p_avrent_first'].isnull() == True) & (data['p_avrent_second'].isnull() == True) & (data['currtag'] == 0), 1, 0)
    data['diff_rent'] = np.where((data['ind_avrent'].isnull() == False) & ((data['p_avrent_first'].isnull() == False) | (data['p_avrent_second'].isnull() == False)) & (data['ind_avrent'] != data['p_avrent_first']) & (data['ind_avrent'] != data['p_avrent_second']), 1, 0)
    data['removed_rent'] = np.where((data['ind_avrent'].isnull() == True) & (data['p_avrent_first'].isnull() == False) & (data['count_same_day_survs'] == 1), 1, 0)
    data['removed_rent'] = np.where((data['count_same_day_rent'] < 2) & (data['p_avrent_first'].isnull() == False) & (data['p_avrent_second'].isnull() == False), 1, data['removed_rent'])

    data['count_same_day_opex'] = data.groupby(['realid', 'survdate'])['op_exp'].transform('count')

    data['new_opex'] = np.where((data['op_exp'].isnull() == False) & (data['p_opex_first'].isnull() == True) & (data['p_opex_second'].isnull() == True) & (data['currtag'] == 0), 1, 0)
    data['diff_opex'] = np.where((data['op_exp'].isnull() == False) & ((data['p_opex_first'].isnull() == False) | (data['p_opex_second'].isnull() == False)) & (data['op_exp'] != data['p_opex_first']) & (data['op_exp'] != data['p_opex_second']), 1, 0)
    data['removed_opex'] = np.where((data['op_exp'].isnull() == True) & (data['p_opex_first'].isnull() == False) & (data['count_same_day_survs'] == 1), 1, 0)
    data['removed_opex'] = np.where((data['count_same_day_opex'] < 2) & (data['p_opex_first'].isnull() == False) & (data['p_opex_second'].isnull() == False), 1, data['removed_opex'])

    data['count_same_day_retax'] = data.groupby(['realid', 'survdate'])['re_tax'].transform('count')

    data['new_tax'] = np.where((data['re_tax'].isnull() == False) & (data['p_retax_first'].isnull() == True) & (data['p_retax_second'].isnull() == True) & (data['currtag'] == 0), 1, 0)
    data['diff_tax'] = np.where((data['re_tax'].isnull() == False) & ((data['p_retax_first'].isnull() == False) | (data['p_retax_second'].isnull() == False)) & (data['re_tax'] != data['p_retax_first']) & (data['re_tax'] != data['p_retax_second']), 1, 0)
    data['removed_tax'] = np.where((data['re_tax'].isnull() == True) & (data['p_retax_first'].isnull() == False) & (data['count_same_day_survs'] == 1), 1, 0)
    data['removed_tax'] = np.where((data['count_same_day_retax'] < 2) & (data['p_retax_first'].isnull() == False) & (data['p_retax_second'].isnull() == False), 1, data['removed_tax'])  

    data['count_same_day_avail'] = data.groupby(['realid', 'survdate'])['ind_avail'].transform('count')

    data['new_avail'] = np.where((data['ind_avail'].isnull() == False) & (data['p_avail_first'].isnull() == True) & (data['p_avail_second'].isnull() == True) & (data['currtag'] == 0), 1, 0)
    data['diff_avail'] = np.where((data['ind_avail'].isnull() == False) & ((data['p_avail_first'].isnull() == False) | (data['p_avail_second'].isnull() == False)) & (data['ind_avail'] != data['p_avail_first']) & (data['ind_avail'] != data['p_avail_second']), 1, 0)
    data['removed_avail'] = np.where((data['ind_avail'].isnull() == True) & (data['p_avail_first'].isnull() == False) & (data['count_same_day_survs'] == 1), 1, 0)
    data['removed_avail'] = np.where((data['count_same_day_avail'] < 2) & (data['p_avail_first'].isnull() == False) & (data['p_avail_second'].isnull() == False), 1, data['removed_avail'])  


    data_filt = data.copy()
    data_filt['flag_period'] = data_filt['reismon'].astype(int).astype(str) + "/" + data_filt['reisyr'].astype(int).astype(str)
    data_filt = data_filt[['realid', 'metcode', 'subid', 'type2', 'survdate', 'reisyr', 'reismon', 
                            'ind_avrent', 'rnt_term', 'p_avrent_first', 'p_avrent_second', 'new_rent', 'diff_rent', 'removed_rent', 
                            'op_exp', 'p_opex_first', 'p_opex_second', 'new_opex', 'diff_opex', 'removed_opex',
                            're_tax', 'p_retax_first', 'p_retax_second', 'new_tax', 'diff_tax', 'removed_tax', 'ind_avail', 'p_avail_first', 'p_avail_second', 'new_avail', 'diff_avail', 'removed_avail', 'flag_period']]
    
    data_filt = data_filt[(data_filt['new_rent'] == 1) | (data_filt['diff_rent'] == 1) | (data_filt['removed_rent'] == 1) |
                          (data_filt['new_opex'] == 1) | (data_filt['diff_opex'] == 1) | (data_filt['removed_opex'] == 1) |
                          (data_filt['new_tax'] == 1) | (data_filt['diff_tax'] == 1) | (data_filt['removed_tax'] == 1) |
                          (data_filt['new_avail'] == 1) | (data_filt['diff_avail'] == 1) | (data_filt['removed_avail'] == 1)]

    data_filt.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/new_survs.pickle'.format(sector_val, curryr, currmon))

    data_filt['type2'] = np.where(data_filt['type2'] == "F", "F", "DW")
    data_filt['identity'] = data_filt['metcode'] + data_filt['subid'].astype(int).astype(str) + data_filt['type2']
    data_filt['identity_met'] = data_filt['metcode'] + data_filt['type2']

    sub_levels = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sub_levels.pickle'.format(sector_val, curryr, currmon))
    met_levels = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/metro_avgs.pickle'.format(sector_val, curryr, currmon))
    
    sub_levels = sub_levels.rename(columns={'uniq1': 'identity'})
    for col in list(sub_levels.columns):
        if col != "identity" and col[-3:] != "sub": 
            sub_levels.rename(columns={col: col + "_sub"}, inplace=True)
    data_filt = data_filt.join(sub_levels.set_index('identity'), on='identity')

    met_levels = met_levels.rename(columns={'uniq2': 'identity_met'})
    met_levels = met_levels[['identity_met', 'renx_max_met', 'renx_min_met', 'renx_avg_met', 'renx_sd_met', 'renx_count_met', 'renx_count_uniq_met']]
    data_filt = data_filt.join(met_levels.set_index('identity_met'), on='identity_met')
    
    data_filt['net_rent'] = np.where((data_filt['rnt_term'] == "G") & (data_filt['op_exp'].isnull() == False), data_filt['ind_avrent'] - data_filt['op_exp'], data_filt['ind_avrent'])
    data_filt['net_rent'] = np.where((data_filt['rnt_term'] == "G") & (data_filt['op_exp'].isnull() == True), data_filt['ind_avrent'] - (data_filt['ind_avrent'] * 0.3), data_filt['net_rent'])
    
    data_filt['rent_level_flag'] = np.where((data_filt['new_rent'] == 1) & ((data_filt['net_rent'] == data_filt['renx_min_sub']) | (data_filt['net_rent'] < data_filt['renx_avg_sub'] - (data_filt['renx_sd_sub'] * 1.5))), 1, 0)
    data_filt['rent_level_flag'] = np.where((data_filt['new_rent'] == 1) & ((data_filt['net_rent'] == data_filt['renx_max_sub']) | (data_filt['net_rent'] > data_filt['renx_avg_sub'] + (data_filt['renx_sd_sub'] * 1.5))), 1, data_filt['rent_level_flag'])
    
    data_filt['opex_level_flag'] = np.where((data_filt['new_opex'] == 1) & (data_filt['op_exp'] < data_filt['avg_op_exp_sub'] - (data_filt['std_op_exp_sub'] * 1.5)), 1, 0)
    data_filt['opex_level_flag'] = np.where((data_filt['new_opex'] == 1) & (data_filt['op_exp'] > data_filt['avg_op_exp_sub'] + (data_filt['std_op_exp_sub'] * 1.5)), 1, data_filt['opex_level_flag'])
    
    data_filt['tax_level_flag'] = np.where((data_filt['new_tax'] == 1) & (data_filt['re_tax'] < data_filt['avg_re_tax_sub'] - (data_filt['std_re_tax_sub'] * 1.5)), 1, 0)
    data_filt['tax_level_flag'] = np.where((data_filt['new_tax'] == 1) & (data_filt['re_tax'] > data_filt['avg_re_tax_sub'] + (data_filt['std_re_tax_sub'] * 1.5)), 1, data_filt['tax_level_flag'])


    data_filt = data_filt[((data_filt['new_rent'] == 1) & (data_filt['rent_level_flag'] == 1)) | (data_filt['diff_rent'] == 1) | (data_filt['removed_rent'] == 1) | 
                          ((data_filt['new_opex'] == 1) & (data_filt['opex_level_flag'] == 1)) | (data_filt['diff_opex'] == 1) | (data_filt['removed_opex'] == 1) |
                          ((data_filt['new_tax'] == 1) & (data_filt['tax_level_flag'] == 1)) | (data_filt['diff_tax'] == 1) | (data_filt['removed_tax'] == 1) |
                          (data_filt['new_avail'] == 1) | (data_filt['diff_avail'] == 1) | (data_filt['removed_avail'] == 1)]

    flag_cols = ['new_rent', 'diff_rent', 'removed_rent', 'new_opex', 'diff_opex', 'removed_opex', 'new_tax', 'diff_tax', 'removed_tax', 'new_avail', 'diff_avail', 'removed_avail']

    
    data_filt = data_filt.rename(columns={'realid': 'id'})
    
    data_filt.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/surv_chgs.pickle'.format(sector_val, curryr, currmon))

    return flag_cols