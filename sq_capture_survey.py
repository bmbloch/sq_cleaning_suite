# Use this program to identify cases where the square code is not using surveyed data for key variables

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
pd.options.display.float_format = '{:.3f}'.format
from IPython.display import display, HTML

def sq_capture_survey_flags(sector_val, curryr, currmon, msq_data_in):

    # Load in the aggregated log file data. This data was saved by the Load_Logs program.
    data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))

    # Load the list of ids with comma errors
    with open('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/error_list.csv'.format(sector_val, curryr, currmon), newline='') as f:
        reader = csv.reader(f)
        id_error_list = list(reader)

    data_filt = data.copy()
    data_filt['identity_survdate'] = data_filt['realid'].astype(str) + data_filt['reisyr'].astype(str) + data_filt['reismon'].astype(str)
    data_filt['survday'] = np.where(data_filt['survday'] < 10, '0' + data_filt['survday'].astype(str), data_filt['survday'].astype(str))
    data_filt['identity_mon'] = data_filt['realid'].astype(str) + data_filt['reisyr'].astype(str) + data_filt['reismon'].astype(str)
    data_filt['identity_day'] = data_filt['realid'].astype(str) + data_filt['reisyr'].astype(str) + data_filt['reismon'].astype(str) + data_filt['survday'].astype(str)
    filt_list = ['realid', 'reisyr', 'reismon', 'survday', 'identity_survdate', 'identity_mon', 'identity_day', 'ind_avrent', 'op_exp', 're_tax', 'ind_avail', 'sublet', 'rnt_term']
    data_filt = data_filt[filt_list]

    data_filt = data_filt.rename(columns={'ind_avrent': 's_avrent', 
                                        'op_exp': 's_opex', 
                                        're_tax': 's_tax', 
                                        'ind_avail': 's_avail', 
                                        'sublet': 's_sublet', 
                                        'rnt_term': 's_rnt_term'})

        
    data_filt = data_filt.replace('', np.nan)

    data_filt.sort_values(by=['realid', 'reisyr', 'reismon', 'survday', 's_avail', 's_sublet'], ascending=[True, False, False, False, False, False] , inplace=True)

    # Identify cases where there were 2 surveys in a month so that we can unflag these cases if squaring has chosen one of the values
    def surv_same_mon(data_in, surv_var, new_col_name):
        for it, sort_order in zip(['_first', '_second'], [False, True]):
            data_same_mon = data_in.copy()
            data_same_mon.sort_values(by=['realid', 'reisyr', 'reismon', surv_var], ascending=[True, False, False, sort_order], inplace=True)
            data_same_mon['count'] = data_same_mon.groupby('identity_mon')['realid'].transform('count')
            data_same_mon = data_same_mon[data_same_mon['count'] > 1]
            data_same_mon = data_same_mon.drop_duplicates(['realid', 'reisyr', 'reismon'])
            data_same_mon = data_same_mon.set_index('identity_mon')
            data_same_mon = data_same_mon[[surv_var]]
            data_same_mon = data_same_mon.rename(columns={surv_var: new_col_name + it})
            data_in = data_in.join(data_same_mon, on='identity_mon')

        return data_in

    data_filt = surv_same_mon(data_filt, 's_avrent', 'same_mon_rent_tag')
    data_filt = surv_same_mon(data_filt, 's_opex', 'same_mon_opex_tag')
    data_filt = surv_same_mon(data_filt, 's_tax', 'same_mon_tax_tag')
    data_filt = surv_same_mon(data_filt, 's_avail', 'same_mon_avail_tag')
    data_filt = surv_same_mon(data_filt, 's_sublet', 'same_mon_sublet_tag')
    data_filt = surv_same_mon(data_filt, 's_rnt_term', 'same_mon_rnt_term_tag')

    data_filt[['s_avrent', 's_opex', 's_tax']] = data_filt.groupby('identity_survdate')[['s_avrent', 's_opex', 's_tax']].transform('mean')
    data_filt[['s_avrent', 's_opex', 's_tax']] = round(data_filt[['s_avrent', 's_opex', 's_tax']], 2)

    data_filt = data_filt.fillna(-1)
    data_filt['sublet_tag'] = np.where((data_filt['realid'] == data_filt['realid'].shift(1)) & (data_filt['s_sublet'] == -1) & (data_filt['s_sublet'].shift(1) > -1) & (data_filt['reisyr'] == data_filt['reisyr'].shift(1)) & (data_filt['reismon'] == data_filt['reismon'].shift(1)), data_filt['s_sublet'] + data_filt['s_sublet'].shift(1) + 1, np.nan)
    data_filt['sublet_tag'] = np.where((data_filt['realid'] == data_filt['realid'].shift(-1)) & (data_filt['s_sublet'] == -1) & (data_filt['s_sublet'].shift(-1) > -1) & (data_filt['reisyr'] == data_filt['reisyr'].shift(-1)) & (data_filt['reismon'] == data_filt['reismon'].shift(-1)), data_filt['s_sublet'] + data_filt['s_sublet'].shift(-1) + 1, data_filt['sublet_tag'])
    data_filt = data_filt.replace(-1, np.nan)
    data_filt = data_filt.drop(['realid', 'reisyr', 'reismon', 'survday'], axis=1)


    data_filt = data_filt.drop_duplicates('identity_survdate')
    data_filt = data_filt.set_index('identity_survdate')

    # Join the surveyed data from the log files to the msq dataframe
    data_msq = msq_data_in.copy()
    data_msq['realid'] = data_msq['realid'].astype(int)
    data_msq['yr'] = data_msq['yr'].astype(int)
    data_msq['currmon'] = data_msq['currmon'].astype(int)

    data_msq['identity_survdate'] = data_msq['realid'].astype(str) + data_msq['yr'].astype(float).astype(str) + data_msq['currmon'].astype(float).astype(str)
    data_msq = data_msq.set_index('identity_survdate')
    data_msq = data_msq.join(data_filt)
    data_msq['availx'] = data_msq['availx'].astype(float)
    data_msq['sublet'] = data_msq['sublet'].astype(float)

    # Identify cases where the msqs do not reflect the surveyed value for key vars, and where the xm column is not correct
    def test_surv(data_msq, surv_var, msq_var, xm_var, flag_cols, prefix):
        if surv_var == "s_avrent":
            data_msq[prefix + surv_var] = np.where((data_msq[surv_var].isnull() == False) & (data_msq['rnt_term'] == "N") & (data_msq[msq_var] != data_msq[surv_var]), 1, 0)
            data_msq[prefix + surv_var] = np.where((data_msq[surv_var].isnull() == False) & (data_msq['rnt_term'] == "G") & (round(data_msq[msq_var] + data_msq['opex'],2) != data_msq[surv_var]), 1, data_msq[prefix + surv_var])
        else:
            data_msq[prefix + surv_var] = np.where((data_msq[surv_var].isnull() == False) & (data_msq[msq_var] != data_msq[surv_var]), 1, 0)
        flag_cols.append(prefix + surv_var)
        
        if surv_var != 's_rnt_term':
            data_msq[prefix + surv_var + '_xm'] = np.where(((data_msq[surv_var].isnull() == False) & (data_msq[xm_var] == 1)) | ((data_msq[surv_var].isnull() == True) & (data_msq[xm_var] == 0)), 1, 0)
            flag_cols.append(prefix + surv_var + '_xm')
        
        return data_msq, flag_cols

    # Call the function to test
    flag_cols = []
    data_msq, flag_cols = test_surv(data_msq, 's_avrent', 'renx', 'renxM', flag_cols, "r_flag_")
    data_msq, flag_cols = test_surv(data_msq, 's_opex', 'opex', 'opexM', flag_cols, "r_flag_")
    data_msq, flag_cols = test_surv(data_msq, 's_tax', 'taxx', 'taxxM', flag_cols, "r_flag_")
    data_msq, flag_cols = test_surv(data_msq, 's_avail', 'availx', 'availxM', flag_cols, "v_flag_")
    data_msq, flag_cols = test_surv(data_msq, 's_sublet', 'sublet', 'subletxM', flag_cols, "v_flag_")
    data_msq, flag_cols = test_surv(data_msq, 's_rnt_term', 'rnt_term', False, flag_cols, "r_flag_")

    # Calculate the average surveyed opex for each property
    data_msq['avg_s_opex'] = data_msq.groupby('id')['s_opex'].transform('mean')

    # Calculate the surveyed opex to gross rent ratio
    data_msq['o_r_ratio'] = np.where((np.isnan(data_msq['s_avrent']) == False) & (np.isnan(data_msq['s_opex']) == False), data_msq['s_avrent'] / (data_msq['s_opex'] + data_msq['s_avrent']), np.nan)

    # Calculate the unnet square rent
    data_msq['renx_unnet'] = data_msq['renx'] + data_msq['opex']

    # Remove flags for certain data entry exceptions where the square code is handling them correctly despite the surveyed value
    data_msq['r_flag_s_avrent'] = np.where((data_msq['s_avrent'] == 0), 0, data_msq['r_flag_s_avrent'])
    data_msq['r_flag_s_avrent'] = np.where((data_msq['rnt_term'] == "G") & (abs(data_msq['s_avrent'] - (data_msq['renx'] + data_msq['opex'])) <= 0.01001) & (abs(data_msq['s_avrent'] - (data_msq['renx'] + data_msq['opex'])) > 0), 0, data_msq['r_flag_s_avrent'])
    data_msq['r_flag_s_avrent'] = np.where((data_msq['rnt_term'] == "N") & (abs(data_msq['s_avrent'] - data_msq['renx']) <= 0.01001) & (abs(data_msq['s_avrent'] - data_msq['renx']) > 0), 0, data_msq['r_flag_s_avrent'])
    data_msq['r_flag_s_avrent'] = np.where((data_msq['r_flag_s_avrent'] == 1) & (np.isnan(data_msq['renx']) == False) & (np.isnan(data_msq['same_mon_rent_tag_first']) == False) & (data_msq['rnt_term'] == "N") & ((abs(data_msq['same_mon_rent_tag_first'] - data_msq['renx']) < 0.0001) | (abs(data_msq['same_mon_rent_tag_second'] - data_msq['renx']) < 0.0001)), 0, data_msq['r_flag_s_avrent'])
    data_msq['r_flag_s_avrent'] = np.where((data_msq['r_flag_s_avrent'] == 1) & (np.isnan(data_msq['renx']) == False) & (np.isnan(data_msq['same_mon_rent_tag_first']) == False) & (data_msq['rnt_term'] == "G") & ((abs(data_msq['same_mon_rent_tag_first'] - (data_msq['renx'] + data_msq['opex'])) < 0.001) | (abs(data_msq['same_mon_rent_tag_second'] - (data_msq['renx'] + data_msq['opex'])) < 0.001)), 0, data_msq['r_flag_s_avrent'])
    
    data_msq['r_flag_s_avrent_xm'] = np.where((data_msq['r_flag_s_avrent_xm'] == 1) & (np.isnan(data_msq['same_mon_rent_tag_first']) == False), 0, data_msq['r_flag_s_avrent_xm'])

    data_msq['r_flag_s_opex'] = np.where((abs(data_msq['s_opex'] - data_msq['opex']) <= 0.01001) & (abs(data_msq['s_opex'] - data_msq['opex']) > 0), 0, data_msq['r_flag_s_opex'])
    data_msq['r_flag_s_opex'] = np.where((data_msq['r_flag_s_opex'] == 1) & (data_msq['o_r_ratio'] >= 0.5), 0, data_msq['r_flag_s_opex'])
    data_msq['r_flag_s_opex'] = np.where((data_msq['r_flag_s_opex'] == 1) & ((data_msq['s_opex'] / data_msq['renx'] >= 0.8) | (data_msq['s_opex'] / data_msq['renx'] <= 0.1)), 0, data_msq['r_flag_s_opex'])
    data_msq['r_flag_s_opex'] = np.where((data_msq['s_opex'] == 0), 0, data_msq['r_flag_s_opex'])
    data_msq['r_flag_s_opex'] = np.where((data_msq['s_opex'] > data_msq['avrent']), 0, data_msq['r_flag_s_opex'])
    data_msq['r_flag_s_opex'] = np.where((data_msq['r_flag_s_opex'] == 1) & (np.isnan(data_msq['opex']) == False) & (np.isnan(data_msq['same_mon_opex_tag_first']) == False) & ((data_msq['same_mon_opex_tag_first'] == data_msq['opex']) | (data_msq['same_mon_opex_tag_second'] == data_msq['opex'])), 0, data_msq['r_flag_s_opex'])

    data_msq['r_flag_s_opex_xm'] = np.where((data_msq['s_opex'] == 0), 0, data_msq['r_flag_s_opex_xm'])
    data_msq['r_flag_s_opex_xm'] = np.where((data_msq['s_opex'] > data_msq['avrent']), 0, data_msq['r_flag_s_opex_xm'])
    data_msq['r_flag_s_opex_xm'] = np.where((data_msq['r_flag_s_opex_xm'] == 1) & (np.isnan(data_msq['same_mon_opex_tag_first']) == False), 0, data_msq['r_flag_s_opex_xm'])

    data_msq['r_flag_s_tax'] = np.where((data_msq['s_tax'] == 0), 0, data_msq['r_flag_s_tax'])
    data_msq['r_flag_s_tax'] = np.where((abs(data_msq['s_tax'] - data_msq['taxx']) <= 0.01001) & (abs(data_msq['s_tax'] - data_msq['taxx']) > 0), 0, data_msq['r_flag_s_tax'])
    data_msq['r_flag_s_tax'] = np.where((data_msq['r_flag_s_tax'] == 1) & (np.isnan(data_msq['taxx']) == False) & (np.isnan(data_msq['same_mon_tax_tag_first']) == False) & ((data_msq['same_mon_tax_tag_first'] == data_msq['taxx']) | (data_msq['same_mon_tax_tag_second'] == data_msq['taxx'])), 0, data_msq['r_flag_s_tax'])

    data_msq['r_flag_s_tax_xm'] = np.where((data_msq['s_tax'] == 0), 0, data_msq['r_flag_s_tax_xm'])
    data_msq['r_flag_s_tax_xm'] = np.where((data_msq['r_flag_s_tax_xm'] == 1) & (np.isnan(data_msq['same_mon_tax_tag_first']) == False), 0, data_msq['r_flag_s_tax_xm'])

    data_msq['v_flag_s_avail'] = np.where((data_msq['s_avail'] > data_msq['ind_size']) & (data_msq['availx'] == data_msq['ind_size']), 0, data_msq['v_flag_s_avail'])
    data_msq['v_flag_s_avail'] = np.where((data_msq['v_flag_s_avail'] == 1) & (np.isnan(data_msq['availx']) == False) & (np.isnan(data_msq['same_mon_avail_tag_first']) == False) & ((data_msq['same_mon_avail_tag_first'] == data_msq['availx']) | (data_msq['same_mon_avail_tag_second'] == data_msq['availx'])), 0, data_msq['v_flag_s_avail'])
    data_msq['v_flag_s_avail'] = np.where((data_msq['v_flag_s_avail'] == 1) & (data_msq['s_avail'] == data_msq['sublet']) & (data_msq['s_avail'] == data_msq['ind_size']), 0, data_msq['v_flag_s_avail'])
    data_msq['v_flag_s_avail'] = np.where((data_msq['v_flag_s_avail'] == 1) & (data_msq['s_avail'] < 0) & (data_msq['availx'] == 0), 0, data_msq['v_flag_s_avail'])
    data_msq['v_flag_s_avail'] = np.where((data_msq['v_flag_s_avail'] == 1) & (data_msq['s_avail'] + data_msq['sublet'] > data_msq['ind_size']) & (data_msq['totavailx'] == data_msq['ind_size']), 0, data_msq['v_flag_s_avail'])
    data_msq['v_flag_s_avail'] = np.where((data_msq['v_flag_s_avail'] == 1) & (data_msq['same_mon_avail_tag_first'].isnull() == False) & (data_msq['same_mon_avail_tag_first'] + data_msq['sublet'] > data_msq['ind_size']) & (data_msq['totavailx'] == data_msq['ind_size']), 0, data_msq['v_flag_s_avail'])
    data_msq['v_flag_s_avail'] = np.where((data_msq['v_flag_s_avail'] == 1) & (data_msq['same_mon_avail_tag_second'].isnull() == False) & (data_msq['same_mon_avail_tag_second'] + data_msq['sublet'] > data_msq['ind_size']) & (data_msq['totavailx'] == data_msq['ind_size']), 0, data_msq['v_flag_s_avail'])

    data_msq['v_flag_s_avail_xm'] = np.where((data_msq['v_flag_s_avail_xm'] == 1) & (np.isnan(data_msq['same_mon_avail_tag_first']) == False), 0, data_msq['v_flag_s_avail_xm'])
    data_msq['v_flag_s_avail_xm'] = np.where((data_msq['v_flag_s_avail_xm'] == 1) & (data_msq['s_avail'] < 0) & (data_msq['availx'] == 0), 0, data_msq['v_flag_s_avail_xm'])

    data_msq['v_flag_s_sublet'] = np.where((data_msq['s_sublet'] > data_msq['ind_size']) & (data_msq['sublet'] == data_msq['ind_size']), 0, data_msq['v_flag_s_sublet'])
    data_msq['v_flag_s_sublet'] = np.where((data_msq['v_flag_s_sublet'] == 1) & (data_msq['sublet_tag'] == data_msq['sublet']), 0, data_msq['v_flag_s_sublet'])
    data_msq['v_flag_s_sublet'] = np.where((data_msq['v_flag_s_sublet'] == 1) & (np.isnan(data_msq['sublet']) == False) & (np.isnan(data_msq['same_mon_sublet_tag_first']) == False) & ((data_msq['same_mon_sublet_tag_first'] == data_msq['sublet']) | (data_msq['same_mon_sublet_tag_second'] == data_msq['sublet'])), 0, data_msq['v_flag_s_sublet'])

    data_msq['v_flag_s_sublet_xm'] = np.where((data_msq['sublet_tag'] == data_msq['sublet']), 0, data_msq['v_flag_s_sublet_xm'])
    data_msq['v_flag_s_sublet_xm'] = np.where((data_msq['v_flag_s_sublet_xm'] == 1) & (np.isnan(data_msq['same_mon_sublet_tag_first']) == False), 0, data_msq['v_flag_s_sublet_xm'])

    data_msq['r_flag_s_rnt_term'] = np.where((data_msq['r_flag_s_rnt_term'] == 1) & (data_msq['rnt_term'].isnull() == False) & (data_msq['same_mon_rnt_term_tag_first'].isnull() == False) & ((data_msq['same_mon_rnt_term_tag_first'] == data_msq['rnt_term']) | (data_msq['same_mon_rnt_term_tag_second'] == data_msq['rnt_term'])), 0, data_msq['r_flag_s_rnt_term'])
    data_msq['r_flag_s_rnt_term'] = np.where((data_msq['rnt_term'] == 'N') & (data_msq['s_rnt_term'] == 'G') & (data_msq['s_opex'] / data_msq['s_avrent'] > 0.65), 0, data_msq['r_flag_s_rnt_term'])
    data_msq['r_flag_s_rnt_term'] = np.where((data_msq['rnt_term'] == 'N') & (data_msq['s_rnt_term'] == 'G') & (data_msq['avg_s_opex'] / data_msq['s_avrent'] > 0.4), 0, data_msq['r_flag_s_rnt_term'])
    
    # Join the ids that are in the error list due to commas in one of the columns to the dataframe and remove their flag tags
    df_list = []
    has_error = False
    for x in id_error_list:
        try:
            df_list.append(int(x[0]))
        except:
            has_error = True
    error_ids = pd.DataFrame({'realid': df_list})
    error_ids['id_error'] = 1
    error_ids = error_ids.set_index('realid')
    data_msq = data_msq.join(error_ids, on='realid')
    for x in flag_cols:
        data_msq[x] = np.where((data_msq['id_error'] == 1), 0, data_msq[x])

    # Trim the dataset so that only those with flags are left
    data_flags = data_msq.copy()
    data_flags['has_flag'] = data_flags[flag_cols].any(1)
    data_flags = data_flags[data_flags['has_flag'] == True]
    data_flags['type2'] = np.where(data_flags['type2'] == "F", "F", "DW")
    data_flags['identity'] = data_flags['metcode'] + data_flags['subid'].astype(str) + data_flags['type2']
    data_flags['flag_period'] = np.where(data_flags['currmon'].isnull() == True, data_flags['qtr'].astype(str) + "/" + data_flags['yr'].astype(str), data_flags['currmon'].astype(str) + "/" + data_flags['qtr'].astype(str) + "/" + data_flags['yr'].astype(str))

    data_flags.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/surv_check_flags.pickle'.format(sector_val, curryr, currmon))


    return flag_cols