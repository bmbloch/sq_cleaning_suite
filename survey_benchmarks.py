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
import matplotlib.pyplot as plt
import os
import re
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.2f}'.format
from IPython.display import display, HTML

def gen_curr_data(data_in, suffix, identity):
    temp = data_in.copy()
    temp = temp[temp['curr_tag'] == 1]
    temp = temp[(temp['term_switch'] == 0) | ((temp['term_switch'] == 1) & (abs(temp['G_avgrenxAdj']) <= 0.05))]
    temp = temp[temp['renxm'] == 0]
    temp = temp[temp['G_avgrenxAdj'].isnull() == False]
    temp['avg_cat_curr_G_renxAdj' + suffix] = temp.groupby(identity)['G_avgrenxAdj'].transform('mean')
    temp['stdev_cat_curr_G_renxAdj' + suffix] = temp.groupby(identity)['G_avgrenxAdj'].transform('std', ddof=1)
    temp['count_cat_curr_G_renxAdj' + suffix] = temp.groupby(identity)['G_avgrenx'].transform('count')
    temp['count_curr_ren' + suffix + '_up'] = temp[temp['G_avgrenx'] > 0].groupby(identity)['G_avgrenx'].transform('count')
    temp['count_curr_ren' + suffix + '_down'] = temp[temp['G_avgrenx'] < 0].groupby(identity)['G_avgrenx'].transform('count')
    temp['count_curr_ren' + suffix + '_zero'] = temp[temp['G_avgrenx'] == 0].groupby(identity)['G_avgrenx'].transform('count')
    temp[['count_curr_ren' + suffix + '_up', 'count_curr_ren' + suffix + '_down', 'count_curr_ren' + suffix + '_zero']] = temp.groupby(identity)[['count_curr_ren' + suffix + '_up', 'count_curr_ren' + suffix + '_down', 'count_curr_ren' + suffix + '_zero']].bfill()
    temp[['count_curr_ren' + suffix + '_up', 'count_curr_ren' + suffix + '_down', 'count_curr_ren' + suffix + '_zero']] = temp.groupby(identity)[['count_curr_ren' + suffix + '_up', 'count_curr_ren' + suffix + '_down', 'count_curr_ren' + suffix + '_zero']].ffill()
    temp[['avg_cat_curr_G_renxAdj' + suffix, 'stdev_cat_curr_G_renxAdj' + suffix]] = round(temp[['avg_cat_curr_G_renxAdj' + suffix, 'stdev_cat_curr_G_renxAdj' + suffix]], 3)
    temp = temp[[identity, 'avg_cat_curr_G_renxAdj' + suffix, 'stdev_cat_curr_G_renxAdj' + suffix, 'count_cat_curr_G_renxAdj' + suffix, 'count_curr_ren' + suffix + '_up', 'count_curr_ren' + suffix + '_down', 'count_curr_ren' + suffix + '_zero']]
    temp = temp.drop_duplicates(identity)
    temp = temp.set_index(identity)
    data_in = data_in.join(temp, on=identity)

    return data_in

def process_survey_benchmarks(sector_val, curryr, currmon):

    # Load in the aggregated log file data. This data was saved by the Load_Logs program.
    data_in = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))

    data = data_in.copy()
    data = data.rename(columns={'realid': 'id', 'realyr': 'yr', 'ind_avrent': 'renx', 'ind_lowrent': 'lowrent', 'ind_hirent': 'hirent'})
    data['renxm'] = np.where(data['renx'].isnull() == False, 0, 1)
    data['availxm'] = np.where(data['ind_avail'].isnull() == False, 0, 1)

    data['year'] = np.where(data['year'].isnull() == True, 1980, data['year'])
    data['month'] = np.where(data['month'].isnull() == True, 6, data['month'])
    data['agetotmo'] = (data['year'] * 12) + data['month']
    data['prop_age'] = ((data['reisyr'] * 12) + data['reismon']) - data['agetotmo']

    expansion_list = ["AA", "AB", "AK", "AN", "AQ", "BD", "BF", "BI", "BR", "BS", "CG", "CM", "CN", "CS", "DC", 
                    "DM", "DN", "EP", "FC", "FM", "FR", "GD", "GN", "GR", "HR", "HL", "HT", "KX", "LL", "LO", 
                    "LR", "LV", "LX", "MW", "NF", "NM", "NO", "NY", "OK", "OM", "PV", "RE", "RO", "SC", "SH", 
                    "SR", "SS", "ST", "SY", "TC", "TM", "TO", "TU", "VJ", "VN", "WC", "WK", "WL", "WS"]    

    rent_comps_only_list = ['BD', 'CG', 'DC', 'HL', 'SS']

    data['type2'] = np.where(data['type2'] == "F", "F", "DW")

    data['expansion'] = np.where(data['metcode'].isin(expansion_list), "Yes", "No")
    data = data[(data['expansion'] == "No") | (data['type2'] == "DW")]

    data = data[(data['reisyr'] < curryr) | ((data['reismon'] <= currmon) & (data['reisyr'] == curryr))]

    data = data[data['subid'].isnull() == False]
    data = data[(data['submkt'].isnull() == False) | (data['submkt'] != '')]
    data = data[(data['subid'] != 77) | (data['metcode'].isin(rent_comps_only_list))]
    data = data[data['submkt'].str.strip().str[0:2] != '99']

    data['avg_op_exp'] = data.groupby('id')['op_exp'].transform('mean')
    data['rnt_term'] = np.where((data['avg_op_exp'] / data['renx'] > 0.4) & (data['rnt_term'].isnull() == False), 'N', data['rnt_term'])
    
    data['curr_tag'] = np.where((data['reisyr'] == curryr) & (data['reismon'] == currmon), 1, 0)

    data['reisqtr'] = np.ceil(data['reismon'] / 3)
    data['q_currtag'] = np.where((data['reisqtr'] == np.ceil(currmon / 3)) & (data['reisyr'] == curryr), 1, 0)

    data['country'] = "US"

    data['identity'] = data['id'].astype(str) + "/" + data['survdate'].astype(str)

    data['surv_num'] = data.groupby('id')['renx'].transform('count')
    data['first_surv'] = np.where((data['surv_num'] == 1) & (data['curr_tag'] == 1), 1, 0)

    data.sort_values(by=['id', 'reisyr', 'reismon', 'survyr', 'survmon', 'survday', 'renx', 'ind_avail'], ascending=[True, True, True, True, True, True, True, True], inplace=True)

    temp = data.copy()
    temp = temp[(temp['sublet'].isnull() == False) & (temp['curr_tag'] == 0)]
    temp['l_sublet'] = temp.groupby('id').tail(1)['sublet']
    temp = temp[temp['l_sublet'].isnull() == False]
    temp = temp.set_index('id')
    temp = temp[['l_sublet']]
    data = data.join(temp, on='id')

    temp = data.copy()
    temp = temp[(temp['ind_avail'].isnull() == False) & (temp['curr_tag'] == 0)]
    temp['l_ind_avail'] = temp.groupby('id').tail(1)['ind_avail']
    temp['l_ind_avail_date'] = temp.groupby('id').tail(1)['survdate']
    temp = temp[temp['l_ind_avail'].isnull() == False]
    temp = temp.set_index('id')
    temp = temp[['l_ind_avail', 'l_ind_avail_date']]
    data = data.join(temp, on='id')

    temp = data.copy()
    temp = temp[(temp['ind_avail'].isnull() == False) & (temp['curr_tag'] == 0) & (temp['ind_avail'] != 0)]
    temp['last_vac'] = temp.groupby('id').tail(1)['ind_avail']
    temp = temp[temp['last_vac'].isnull() == False]
    temp = temp.set_index('id')
    temp = temp[['last_vac']]
    data = data.join(temp, on='id')

    data['curr_ind_avail'] = np.where((data['ind_avail'].isnull() == False) & (data['curr_tag'] == 1), data['ind_avail'], np.nan)

    data['avail_sub'] = np.where((data['curr_ind_avail'].isnull() == False) & (data['sublet'].isnull() == False), data['curr_ind_avail'] + data['sublet'], np.nan)
    data['avail_sub'] = np.where((data['curr_ind_avail'].isnull() == False) & (data['l_sublet'].isnull() == False) & (data['avail_sub'].isnull() == True), data['curr_ind_avail'] + data['l_sublet'], data['avail_sub'])
    data['avail_sub'] = np.where((data['curr_ind_avail'].isnull() == False) & (data['avail_sub'].isnull() == True), data['curr_ind_avail'], data['avail_sub'])
    data['avail_sub'] = np.where((data['sublet'].isnull() == False) & (data['avail_sub'].isnull() == True), data['sublet'] + data['l_ind_avail'], data['avail_sub'])

    data['l_avail_sub'] = np.where((data['l_sublet'].isnull() == False), data['l_ind_avail'] + data['l_sublet'], np.nan)
    data['l_avail_sub'] = np.where((data['l_avail_sub'].isnull() == True), data['l_ind_avail'], data['l_avail_sub'])
    data['l_avail_sub'] = np.where((data['l_sublet'].isnull() == False) & (data['l_avail_sub'].isnull() == True), data['l_sublet'], data['l_avail_sub'])

    data['d_ind_avail'] = np.where(data['l_avail_sub'].isnull() == False, data['avail_sub'] - data['l_avail_sub'], np.nan)

    data['count_term'] = data.groupby('id')['rnt_term'].transform('count')
    data['term_temp'] = data.groupby('id')['rnt_term'].bfill()
    data['term_est'] = np.where((data['id'] != data['id'].shift(1)) & (data['rnt_term'].isnull() == True), "N", data['rnt_term'])
    data['term_est'] = np.where((data['count_term'] == 1) & (data['surv_num'] > 0) & (data['id'] != data['id'].shift(1)), data['term_temp'], data['term_est'])
    data['term_est'] = data.groupby('id')['term_est'].ffill()
    data['term_est'] = data.groupby('id')['term_est'].bfill()

    temp = data.copy()
    temp = temp[temp['rnt_term'].isnull() == False]
    temp['mr_rnt_term'] = temp.groupby('id').tail(1)['rnt_term']
    temp = temp[temp['mr_rnt_term'].isnull() == False]
    temp = temp.set_index('id')
    temp = temp[['mr_rnt_term']]
    data = data.join(temp, on='id')

    data['term_switch'] = np.where((data['id'] == data['id'].shift(1)) & (data['term_est'] != data['term_est'].shift(1)) & (data['first_surv'] != 1), 1, 0)

    data['uniq1'] = data['metcode'] + data['subid'].astype(int).astype(str) + data['type2']
    data['uniq2'] = data['metcode'] + data['type2']

    temp = data.copy()
    temp = temp[(temp['renx'].isnull() == False) & (temp['curr_tag'] == 0)]
    temp['l_renx'] = temp.groupby('id').tail(1)['renx']
    temp['l_survdate'] = temp.groupby('id').tail(1)['survdate']
    temp['l_rnt_term'] = temp.groupby('id').tail(1)['term_est']
    temp = temp[temp['l_renx'].isnull() == False]
    temp = temp.set_index('id')
    temp = temp[['l_renx', 'l_survdate', 'l_rnt_term']]
    data = data.join(temp, on='id')

    start = datetime(curryr, currmon, 1)
    data['diff_mon'] = (start.year - data['survdate'].dt.year) * 12 + (start.month - data['survdate'].dt.month)
    data['diff_mon'] = np.where(data['diff_mon'] == 0, 1, data['diff_mon'])
    data['renx_unnet_temp'] = np.where((data['term_est'] == "N") & (data['op_exp'].isnull() == False) & (data['renxm'] == 0), data['renx'] + data['op_exp'], np.nan)
    data['renx_unnet_temp'] = np.where((data['term_est'] == "G") & (data['op_exp'].isnull() == False) & (data['renxm'] == 0), data['renx'], data['renx_unnet_temp'])
    data['opex_ren_ratio'] = np.where((data['diff_mon'] <= 60) & (data['renx_unnet_temp'].isnull() == False), data['op_exp'] / data['renx_unnet_temp'], np.nan)
    data['avg_opex_ren_ratio'] = np.mean(data['opex_ren_ratio'])

    data = data.drop(['renx_unnet_temp', 'opex_ren_ratio'], axis=1)

    data['op_exp_temp_f'] = data[data['curr_tag'] == 1].groupby(['id', 'reismon'])['op_exp'].ffill()
    data['op_exp_temp_b'] = data[data['curr_tag'] == 1].groupby(['id', 'reismon'])['op_exp'].bfill()
    data['op_exp'] = np.where((data['op_exp_temp_f'].isnull() == False), data['op_exp_temp_f'], data['op_exp'])
    data['op_exp'] = np.where((data['op_exp_temp_b'].isnull() == False), data['op_exp_temp_b'], data['op_exp'])
    data = data.drop(['op_exp_temp_f', 'op_exp_temp_b'], axis=1)
    data['c_op_exp_ren_ratio'] = np.where(data['curr_tag'] == 1, data['op_exp'] / data['renx'], np.nan)
    data['c_op_exp_ren_ratio'] = data.groupby('id')['c_op_exp_ren_ratio'].bfill()

    temp = data.copy()
    temp = temp[(temp['op_exp'].isnull() == False) & (temp['curr_tag'] == 0) & (temp['op_exp'] != 0)]
    temp['l_op_exp'] = temp.groupby('id').tail(1)['op_exp']
    temp['l_op_exp_survdate'] = temp.groupby('id').tail(1)['survdate']
    temp = temp[temp['l_op_exp'].isnull() == False]
    temp = temp.set_index('id')
    temp = temp[['l_op_exp', 'l_op_exp_survdate']]
    data = data.join(temp, on='id')

    temp = data.copy()
    temp = temp[(temp['op_exp'].isnull() == False) & (temp['curr_tag'] == 0) & (temp['op_exp'] != 0) & (temp['renx'].isnull() == False)]
    temp.sort_values(by=['id', 'reisyr', 'reismon', 'survyr', 'survmon', 'survday', 'renx'], ascending=[True, False, False, False, False, False, False], inplace=True)
    temp = temp.drop_duplicates('id')
    temp['renx_unnet_temp'] = np.where((temp['term_est'] == "N") & (temp['l_op_exp'].isnull() == False) & (temp['op_exp'].isnull() == False) & (temp['renxm'] == 0), temp['renx'] + temp['op_exp'], np.nan)
    temp['renx_unnet_temp'] = np.where((temp['term_est'] == "G") & (temp['l_op_exp'].isnull() == False) & (temp['op_exp'].isnull() == False) & (temp['renxm'] == 0), temp['renx'], temp['renx_unnet_temp'])
    temp['l_op_exp_ren_ratio'] = temp['op_exp'] / temp['renx_unnet_temp']
    temp['l_mr_ratio_totalmo'] = temp['survdate']
    temp = temp[['id', 'l_op_exp_ren_ratio', 'l_mr_ratio_totalmo']]
    temp = temp.set_index('id')
    data = data.join(temp, on='id')

    data['count_l'] = data.groupby('id')['l_op_exp'].transform('count')
    data['curr_no_l'] = np.where(data['count_l'] == 0, data['op_exp'], np.nan)
    data['curr_no_l'] = data.groupby('id')['curr_no_l'].bfill()

    data['l_op_exp_mondiff'] = (start.year - data['l_op_exp_survdate'].dt.year) * 12 + (start.month - data['l_op_exp_survdate'].dt.month)
    data['op_exp_use'] = np.where((data['op_exp'].isnull() == False) & (data['term_est'] == "G") & (data['renx'].isnull() == False) & (data['renx'] > data['op_exp']), data['op_exp'], np.nan)
    data['op_exp_use'] = np.where((data['op_exp_use'].isnull() == True) & (data['c_op_exp_ren_ratio'].isnull() == False) & (data['term_est'] == "G") & (data['renx'].isnull() == False) & (data['diff_mon'] < 18), data['c_op_exp_ren_ratio'] * data['renx'], data['op_exp_use'])
    data['op_exp_use'] = np.where((data['op_exp_use'].isnull() == True) & (data['l_op_exp'].isnull() == False) & (data['term_est'] == "G") & (data['renx'].isnull() == False) & (data['op_exp'].isnull() == True) & (abs(data['l_op_exp_mondiff']) <= 36) & (data['l_op_exp'] < data['renx'] * 0.7), data['l_op_exp'], data['op_exp_use'])
    data['op_exp_use'] = np.where((data['op_exp_use'].isnull() == True) & (data['l_op_exp'].isnull() == False) & (data['term_est'] == "G") & (data['renx'].isnull() == False) & (data['op_exp'].isnull() == True) & (abs(data['l_op_exp_mondiff']) > 36) & (((data['l_op_exp_ren_ratio'] <= 0.8) & (data['l_op_exp_ren_ratio'] >= 0.08)) | (abs(data['op_exp'] - data['l_op_exp']) / data['op_exp'] <= 0.25)), data['l_op_exp_ren_ratio'] * data['renx'], data['op_exp_use'])
    data['op_exp_use'] = np.where((data['op_exp_use'].isnull() == True) & (data['curr_no_l'].isnull() == False) & (data['term_est'] == "G") & (data['renx'].isnull() == False) & (abs(data['diff_mon']) <= 36), data['curr_no_l'], data['op_exp_use'])
    data['op_exp_use'] = np.where((data['op_exp_use'].isnull() == True) & (data['curr_no_l'].isnull() == False) & (data['term_est'] == "G") & (data['renx'].isnull() == False) & (abs(data['diff_mon']) > 36), data['c_op_exp_ren_ratio'] * data['renx'], data['op_exp_use'])
    data['op_exp_use'] = np.where((data['op_exp_use'].isnull() == True) & (data['term_est'] == "G") & (data['renx'].isnull() == False), data['avg_opex_ren_ratio'] * data['renx'], data['op_exp_use'])
    data['op_exp_use'] = round(data['op_exp_use'], 2)
    data['op_expxm'] = np.where(data['op_exp'].isnull() == False, 1, 0)
    opex_g = data.copy()
    opex_g = opex_g[opex_g['op_exp'].isnull() == False]
    opex_g['g_op_exp'] = np.where((opex_g['id'] == opex_g['id'].shift(1)), (opex_g['op_exp'] - opex_g['op_exp'].shift(1)) / opex_g['op_exp'].shift(1), np.nan)
    opex_g['op_exp_mondiff'] = np.where(opex_g['id'] == opex_g['id'].shift(1), (opex_g['survdate'].dt.year - opex_g['survdate'].shift(1).dt.year) * 12 + (opex_g['survdate'].dt.month - opex_g['survdate'].shift(1).dt.month), np.nan)
    opex_g['op_exp_mondiff'] = np.where(opex_g['op_exp_mondiff'] == 0, 1, opex_g['op_exp_mondiff'])
    opex_g['g_op_exp_mon'] = opex_g['g_op_exp'] / opex_g['op_exp_mondiff']       
    opex_g = opex_g[['g_op_exp', 'g_op_exp_mon', 'op_exp_mondiff']]
    data = data.join(opex_g)
                                        
    retax_g = data.copy()
    retax_g = retax_g[retax_g['re_tax'].isnull() == False]
    retax_g['g_re_tax'] = np.where((retax_g['id'] == retax_g['id'].shift(1)), (retax_g['re_tax'] - retax_g['re_tax'].shift(1)) / retax_g['re_tax'].shift(1), np.nan)
    retax_g['re_tax_mon_diff'] = np.where(retax_g['id'] == retax_g['id'].shift(1), (retax_g['survdate'].dt.year - retax_g['survdate'].shift(1).dt.year) * 12 + (retax_g['survdate'].dt.month - retax_g['survdate'].shift(1).dt.month), np.nan)
    retax_g['re_tax_mon_diff'] = np.where(retax_g['re_tax_mon_diff'] == 0, 1, retax_g['re_tax_mon_diff'])
    retax_g['g_re_tax_mon'] = retax_g['g_re_tax'] / retax_g['re_tax_mon_diff']       
    retax_g = retax_g[['g_re_tax', 'g_re_tax_mon']]
    data = data.join(retax_g)

    temp = data.copy()
    temp = temp[(temp['diff_mon'] <= 36)]
    temp['G_avg_op_exp_met'] = temp.groupby('uniq2')['g_op_exp_mon'].transform('mean')
    temp['G_std_op_exp_met'] = temp.groupby('uniq2')['g_op_exp_mon'].transform('std', ddof=1)
    temp['count_G_op_exp_met'] = temp.groupby('uniq2')['g_op_exp_mon'].transform('count')
    temp['G_avg_re_tax_met'] = temp.groupby('uniq2')['g_re_tax_mon'].transform('mean')
    temp['G_std_re_tax_met'] = temp.groupby('uniq2')['g_re_tax_mon'].transform('std', ddof=1)
    temp['count_G_re_tax_met'] = temp.groupby('uniq2')['g_re_tax_mon'].transform('count')
    temp[['G_avg_op_exp_met', 'G_std_op_exp_met', 'G_avg_re_tax_met', 'G_std_re_tax_met']] = round(temp[['G_avg_op_exp_met', 'G_std_op_exp_met', 'G_avg_re_tax_met', 'G_std_re_tax_met']],3)
    temp = temp.drop_duplicates('uniq2')
    temp = temp.set_index('uniq2')
    temp = temp[['G_avg_op_exp_met', 'G_std_op_exp_met', 'count_G_op_exp_met', 'G_avg_re_tax_met', 'G_std_re_tax_met', 'count_G_re_tax_met']]
    data = data.join(temp, on='uniq2')

    temp = data.copy()
    temp = temp[(temp['diff_mon'] <= 36)]
    temp['G_avg_op_exp_us'] = temp['g_op_exp_mon'].mean()
    temp['G_std_op_exp_us'] = temp['g_op_exp_mon'].std(ddof=1)
    temp['count_G_op_exp_us'] = temp['g_op_exp_mon'].count()
    temp['G_avg_re_tax_us'] = temp['g_re_tax_mon'].mean()
    temp['G_std_re_tax_us'] = temp['g_re_tax_mon'].std(ddof=1)
    temp['count_G_re_tax_us'] = temp['g_re_tax_mon'].count()
    temp[['G_avg_op_exp_us', 'G_std_op_exp_us', 'G_avg_re_tax_us', 'G_std_re_tax_us']] = round(temp[['G_avg_op_exp_us', 'G_std_op_exp_us', 'G_avg_re_tax_us', 'G_std_re_tax_us']],3)
    temp = temp.drop_duplicates('country')
    temp = temp.set_index('country')
    temp = temp[['G_avg_op_exp_us', 'G_std_op_exp_us', 'count_G_op_exp_us', 'G_avg_re_tax_us', 'G_std_re_tax_us', 'count_G_re_tax_us']]
    data = data.join(temp, on='country')

    temp = data.copy()
    temp = temp[(temp['diff_mon'] <= 36)]
    temp['avg_op_exp_sub'] = temp.groupby('uniq1')['op_exp'].transform('mean')
    temp['std_op_exp_sub'] = temp.groupby('uniq1')['op_exp'].transform('std', ddof=1)
    temp['count_op_exp_sub'] = temp.groupby('uniq1')['op_exp'].transform('count')
    temp['avg_re_tax_sub'] = temp.groupby('uniq1')['re_tax'].transform('mean')
    temp['std_re_tax_sub'] = temp.groupby('uniq1')['re_tax'].transform('std', ddof=1)
    temp['count_re_tax_sub'] = temp.groupby('uniq1')['re_tax'].transform('count')
    temp[['avg_op_exp_sub', 'std_op_exp_sub', 'avg_re_tax_sub', 'std_re_tax_sub']] = round(temp[['avg_op_exp_sub', 'std_op_exp_sub', 'avg_re_tax_sub', 'std_re_tax_sub']],3)
    temp = temp.drop_duplicates('uniq1')
    temp = temp.set_index('uniq1')
    temp = temp[['avg_op_exp_sub', 'std_op_exp_sub', 'count_op_exp_sub', 'avg_re_tax_sub', 'std_re_tax_sub', 'count_re_tax_sub']]
    data = data.join(temp, on='uniq1')

    data['renx_net'] = np.where((data['term_est'] == "N"), data['renx'], data['renx'] - data['op_exp_use'])
    data['renx_net'] = round(data['renx_net'], 2)


    data.sort_values(by=['id', 'reisyr', 'reismon', 'survyr', 'survmon', 'survday', 'renx_net', 'ind_avail'], ascending=[True, True, True, True, True, True, True, True], inplace=True)
    renx_g = data.copy()
    renx_g = renx_g[renx_g['renx'].isnull() == False]
    renx_g['G_avgrenx'] = np.where((renx_g['id'] == renx_g['id'].shift(1)), (renx_g['renx_net'] - renx_g['renx_net'].shift(1)) / renx_g['renx_net'].shift(1), np.nan)
    renx_g['G_avgrenx'] = round(renx_g['G_avgrenx'],3)
    renx_g['mondiff'] = np.where(renx_g['id'] == renx_g['id'].shift(1), (renx_g['survdate'].dt.year - renx_g['survdate'].shift(1).dt.year) * 12 + (renx_g['survdate'].dt.month - renx_g['survdate'].shift(1).dt.month), np.nan)
    renx_g['mondiff'] = np.where(renx_g['mondiff'] == 0, 1, renx_g['mondiff'])
    renx_g['G_avgrenxAdj'] = renx_g['G_avgrenx'] / renx_g['mondiff']
    renx_g = renx_g[['identity', 'G_avgrenx', 'G_avgrenxAdj', 'mondiff']]
    renx_g = renx_g.drop_duplicates('identity')
    renx_g['G_avgrenxAdj'] = round(renx_g['G_avgrenxAdj'], 3)
    data = data.join(renx_g.set_index('identity'), on='identity')

    data['tier'] = np.where((data['expansion'] == "Yes") & (data['type2'] == "DW"), "Exp_DW", "Leg_DW")
    data['tier'] =  np.where((data['type2'] == "F"), "Leg_F", data['tier'])
    data = gen_curr_data(data, '_us', 'tier')
    data = gen_curr_data(data, '_us_notier', 'type2')
    data = gen_curr_data(data, '_met', 'uniq2')    
    data = gen_curr_data(data, '_sub', 'uniq1')

    temp = data.copy()
    temp = temp[temp['diff_mon'] <= 24]
    temp = temp[temp['G_avgrenxAdj'].isnull() == False]
    temp = temp[temp['G_avgrenxAdj'] <= 2]
    temp['avg_cat_G_renxAdj'] = temp.groupby('uniq2')['G_avgrenxAdj'].transform('mean')
    temp['stdev_cat_G_renxAdj'] = temp.groupby('uniq2')['G_avgrenxAdj'].transform('std', ddof=1)
    temp['count_cat_G_renxAdj'] = temp.groupby('uniq2')['G_avgrenxAdj'].transform('count')
    temp[['avg_cat_G_renxAdj', 'stdev_cat_G_renxAdj']] = round(temp[['avg_cat_G_renxAdj', 'stdev_cat_G_renxAdj']], 3)
    temp = temp[['uniq2', 'avg_cat_G_renxAdj', 'stdev_cat_G_renxAdj', 'count_cat_G_renxAdj']]
    temp = temp.drop_duplicates('uniq2')
    temp = temp.set_index('uniq2')
    data = data.join(temp, on='uniq2')

    temp = data.copy()
    temp = temp[temp['diff_mon'] <= 24]
    temp = temp[temp['renxm'] == 0]
    temp['renx_max'] = temp.groupby('uniq1')['renx_net'].transform('max')
    temp['renx_min'] = temp.groupby('uniq1')['renx_net'].transform('min')
    temp['renx_avg'] = temp.groupby('uniq1')['renx_net'].transform('mean')
    temp['renx_sd'] = temp.groupby('uniq1')['renx_net'].transform('std', ddof=1)
    temp['renx_count'] = temp.groupby('uniq1')['renx_net'].transform('count')
    temp['renx_count_uniq'] = temp.groupby('uniq1')['id'].transform('nunique')
    temp[['renx_max', 'renx_min', 'renx_avg', 'renx_sd', 'renx_count', 'renx_count_uniq']] = round(temp[['renx_max', 'renx_min', 'renx_avg', 'renx_sd', 'renx_count', 'renx_count_uniq']], 2)
    temp = temp[['uniq1', 'renx_max', 'renx_min', 'renx_avg', 'renx_sd', 'renx_count', 'renx_count_uniq']]
    temp = temp.drop_duplicates('uniq1')
    temp = temp.set_index('uniq1')
    data = data.join(temp, on='uniq1')

    temp = data.copy()
    temp = temp[temp['diff_mon'] <= 24]
    temp = temp[temp['renxm'] == 0]
    temp['renx_max_met'] = temp.groupby('uniq2')['renx_net'].transform('max')
    temp['renx_min_met'] = temp.groupby('uniq2')['renx_net'].transform('min')
    temp['renx_avg_met'] = temp.groupby('uniq2')['renx_net'].transform('mean')
    temp['renx_sd_met'] = temp.groupby('uniq2')['renx_net'].transform('std', ddof=1)
    temp['renx_count_met'] = temp.groupby('uniq2')['renx_net'].transform('count')
    temp['renx_count_uniq_met'] = temp.groupby('uniq1')['id'].transform('nunique')
    temp[['renx_max_met', 'renx_min_met', 'renx_avg_met', 'renx_sd_met', 'renx_count_met', 'renx_count_uniq_met']] = round(temp[['renx_max_met', 'renx_min_met', 'renx_avg_met', 'renx_sd_met', 'renx_count_met', 'renx_count_uniq_met']], 2)
    temp = temp[['uniq2', 'renx_max_met', 'renx_min_met', 'renx_avg_met', 'renx_sd_met', 'renx_count_met', 'renx_count_uniq_met']]
    temp = temp.drop_duplicates('uniq2')
    temp = temp.set_index('uniq2')
    data = data.join(temp, on='uniq2')

    temp = data.copy()
    temp = temp[temp['diff_mon'] <= 24]
    temp = temp[temp['renxm'] == 0]
    temp = pd.DataFrame(temp.groupby('uniq1')['renx_net'].quantile(0.95))
    temp.columns = ['pct_95']
    data = data.join(temp, on='uniq1')
    temp = data.copy()
    temp = temp[temp['diff_mon'] <= 24]
    temp = temp[temp['renxm'] == 0]
    temp = pd.DataFrame(temp.groupby('uniq1')['renx_net'].quantile(0.05))
    temp.columns = ['pct_5']
    data = data.join(temp, on='uniq1')

    curr_data = data.copy()
    curr_data = curr_data[curr_data['curr_tag'] == 1]
    curr_data_filt = curr_data.copy()
    curr_data_filt = curr_data_filt[['id', 'survdate', 'uniq1', 'type2', 'survdate', 'metcode', 'ind_size', 'ind_avail', 'renx', 'lowrent', 'hirent',
                                    'l_ind_avail', 'l_ind_avail_date', 'd_ind_avail', 'term_est', 'term_switch', 'op_exp_use',
                                    'renx_net', 'G_avgrenxAdj', 'l_survdate', 'avg_cat_G_renxAdj', 'stdev_cat_G_renxAdj', 'renx_max', 'renx_min', 'renx_avg',
                                    'renx_count', 'renx_count_uniq', 'pct_95', 'pct_5']]
    curr_data_filt.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/curr_data.pickle'.format(sector_val, curryr, currmon))

    metro_data = data.copy()
    metro_data = metro_data[['uniq2', 'avg_cat_G_renxAdj', 'stdev_cat_G_renxAdj', 'count_cat_G_renxAdj', 'avg_cat_curr_G_renxAdj_met', 'stdev_cat_curr_G_renxAdj_met', 'count_cat_curr_G_renxAdj_met',
                            'count_curr_ren_met_up', 'count_curr_ren_met_down', 'count_curr_ren_met_zero', 'renx_max_met', 'renx_min_met',
                            'renx_avg_met', 'renx_sd_met', 'renx_count_met', 'renx_count_uniq_met']]
    metro_data = metro_data.drop_duplicates('uniq2')
    metro_data.sort_values(by=['uniq2'], ascending=[True], inplace=True)
    metro_data.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/metro_avgs.pickle'.format(sector_val, curryr, currmon))

    sub_data = data.copy()
    sub_data = sub_data[['uniq1', 'metcode', 'subid', 'type2', 'avg_cat_curr_G_renxAdj_sub', 'stdev_cat_curr_G_renxAdj_sub', 
                        'count_cat_curr_G_renxAdj_sub', 'count_curr_ren_sub_up', 'count_curr_ren_sub_down', 
                        'count_curr_ren_sub_zero']]
    sub_data = sub_data.drop_duplicates('uniq1')
    sub_data.sort_values(by=['uniq1'], ascending=[True], inplace=True)
    sub_data.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sub_avgs.pickle'.format(sector_val, curryr, currmon))

    sub_levels = data.copy()
    sub_levels = sub_levels[['uniq1', 'renx_max', 'renx_min', 'renx_avg', 'renx_sd', 'renx_count', 'renx_count_uniq', 
                            'avg_op_exp_sub', 'std_op_exp_sub', 'count_op_exp_sub', 'avg_re_tax_sub', 'std_re_tax_sub', 
                            'count_re_tax_sub']]
    sub_levels = sub_levels.drop_duplicates('uniq1')
    sub_levels.sort_values(by=['uniq1'], ascending=[True], inplace=True)
    sub_levels.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sub_levels.pickle'.format(sector_val, curryr, currmon))

    us_data = data.copy()
    us_data = us_data.drop_duplicates('tier')
    us_data.sort_values(by=['tier'], ascending=[True], inplace=True)
    us_data = us_data[['tier', 'avg_cat_curr_G_renxAdj_us', 'stdev_cat_curr_G_renxAdj_us', 
                        'count_cat_curr_G_renxAdj_us', 'count_curr_ren_us_up', 'count_curr_ren_us_down', 
                        'count_curr_ren_us_zero']]
    us_data.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/us_avgs.pickle'.format(sector_val, curryr, currmon))

    us_data_notier = data.copy()
    us_data_notier = us_data_notier.drop_duplicates('type2')
    us_data_notier.sort_values(by=['type2'], ascending=[True], inplace=True)
    us_data_notier = us_data_notier[['type2', 'avg_cat_curr_G_renxAdj_us_notier', 'stdev_cat_curr_G_renxAdj_us_notier', 
                        'count_cat_curr_G_renxAdj_us_notier', 'count_curr_ren_us_notier_up', 'count_curr_ren_us_notier_down', 
                        'count_curr_ren_us_notier_zero']]
    us_data_notier.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/us_avgs_notier.pickle'.format(sector_val, curryr, currmon))

    last_rent = data.copy()
    last_rent.sort_values(by=['id', 'renx'], ascending=[True, False], inplace=True)
    last_rent = last_rent.drop_duplicates(['id', 'reisyr', 'reismon'])
    last_rent.sort_values(by=['id', 'survdate', 'renx_net'], ascending=[True, False, True], inplace=True)
    last_rent['mr_renx'] = np.where((last_rent['curr_tag'] == 1) & (last_rent['renxm'] == 0), last_rent['renx'], last_rent['l_renx'])
    last_rent['mr_survdate'] = np.where((last_rent['curr_tag'] == 1) & (last_rent['renxm'] == 0), last_rent['survdate'], last_rent['l_survdate'])
    last_rent['mr_opex'] = np.where((last_rent['curr_tag'] == 1) & (last_rent['op_exp'].isnull() == False), last_rent['op_exp'], last_rent['l_op_exp'])
    last_rent['mr_survdate_opex'] = np.where((last_rent['curr_tag'] == 1) & (last_rent['op_exp'].isnull() == False), last_rent['survdate'], last_rent['l_op_exp_survdate'])
    last_rent['G_avgrenxAdj'] = np.where(last_rent['curr_tag'] == 0, np.nan, last_rent['G_avgrenxAdj'])
    last_rent['renx'] = np.where(last_rent['curr_tag'] == 0, np.nan, last_rent['renx'])
    last_rent['mr_G_avgrenx'] = last_rent.groupby(['id'])['G_avgrenx'].bfill()
    last_rent['G_avgrenx'] = np.where(last_rent['curr_tag'] == 0, np.nan, last_rent['G_avgrenx'])
    last_rent = last_rent.drop_duplicates('id')
    last_rent = last_rent[['id', 'uniq1', 'renx', 'mr_renx', 'mr_survdate', 'G_avgrenx', 'G_avgrenxAdj', 'mr_G_avgrenx', 'mr_rnt_term', 'l_renx', 'l_rnt_term',
                        'l_survdate', 'mr_opex', 'mr_survdate_opex']]
    last_rent.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/last_rent.pickle'.format(sector_val, curryr, currmon))

    last_vac = data.copy()
    last_vac.sort_values(by=['id', 'ind_avail'], ascending=[True, False], inplace=True)
    last_vac = last_vac.drop_duplicates(['id', 'reisyr', 'reismon'])
    last_vac.sort_values(by=['id', 'survdate', 'ind_avail'], ascending=[True, True, True], inplace=True)
    last_vac = last_vac.drop(['l_survdate'], axis=1)
    last_vac['sublet_fill'] = np.where((last_vac['sublet'].isnull() == True), 0, last_vac['sublet'])
    last_vac['c_ind_avail'] = np.where((last_vac['curr_tag'] == 1) & (last_vac['ind_avail'].isnull() == False), last_vac['ind_avail'] + last_vac['sublet_fill'], np.nan)
    last_vac['c_survdate'] = np.where((last_vac['curr_tag'] == 1) & (last_vac['ind_avail'].isnull() == False), last_vac['survdate'], np.datetime64('NaT'))
    last_vac['ind_avail'] = np.where(last_vac['curr_tag'] == 0, np.nan, last_vac['ind_avail'])
    last_vac['d_ind_avail'] = np.where(last_vac['curr_tag'] == 0, np.nan, last_vac['d_ind_avail'])
    last_vac.sort_values(by=['id', 'reisyr', 'reismon', 'survyr', 'survmon', 'survday', 'ind_avail'], ascending=[True, False, False, False, False, False, False], inplace=True)
    last_vac = last_vac.drop_duplicates('id')
    last_vac = last_vac[['id', 'uniq1', 'c_ind_avail', 'c_survdate', 'l_ind_avail', 'l_avail_sub', 'l_ind_avail_date', 'd_ind_avail']]
    last_vac.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/last_vac.pickle'.format(sector_val, curryr, currmon))