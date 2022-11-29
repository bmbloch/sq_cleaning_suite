import pandas as pd
import numpy as np
from datetime import datetime
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.3f}'.format
from IPython.core.display import display, HTML

def vac_g_flags(sector_val, curryr, currmon, msq_data_in):

    currqtr = int(np.ceil(currmon / 3))

    msq_data = msq_data_in.copy()
    msq_data = msq_data.rename(columns={'id': 'msq_id', 'realid': 'id', 'ind_size': 'sizex'})
    

    msq_data.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)

    msq_data['d_totavailx'] = np.where((msq_data['id'] == msq_data['id'].shift(1)), msq_data['totavailx'] - msq_data['totavailx'].shift(1), np.nan)

    msq_data['type2'] = np.where(msq_data['type2'] == "F", "F", "DW")

    expansion_list = ["AA", "AB", "AK", "AN", "AQ", "BD", "BF", "BI", "BR", "BS", "CG", "CM", "CN", "CS", "DC", 
                    "DM", "DN", "EP", "FC", "FM", "FR", "GD", "GN", "GR", "HR", "HL", "HT", "KX", "LL", "LO", 
                    "LR", "LV", "LX", "MW", "NF", "NM", "NO", "NY", "OK", "OM", "PV", "RE", "RO", "SC", "SH", 
                    "SR", "SS", "ST", "SY", "TC", "TM", "TO", "TU", "VJ", "VN", "WC", "WK", "WL", "WS"]

    msq_data['expansion'] = np.where(msq_data['metcode'].isin(expansion_list), "Yes", "No")
    msq_data = msq_data[(msq_data['expansion'] == "No") | (msq_data['type2'] == "DW")]

    msq_data['msq_id'] = msq_data['msq_id'].astype(int)
    msq_data['identity'] = msq_data['metcode'] + msq_data['subid'].astype(str) + msq_data['type2']
    msq_data['identity_met'] = msq_data['metcode'] + msq_data['type2']
    msq_data['sub_live'] = np.where(msq_data['submkt'].str[0:2] == "99", 0, 1)
    msq_data = msq_data[msq_data['sub_live'] == 1]

    last_vac = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/last_vac.pickle'.format(sector_val, curryr, currmon))
    msq_data = msq_data.join(last_vac.set_index('id'), on='id')

    msq_data.sort_values(by=['msq_id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True])

    temp = msq_data.copy()
    temp['non_surv_flag'] = np.where((temp['d_totavailx'] != 0) & (msq_data['d_totavailx'].isnull() == False) & (temp['availxM'] == 1), 1, 0)
    temp['non_surv_flag'] = np.where((temp['non_surv_flag'] == 1) & (temp['l_ind_avail'].isnull() == True) & (temp['yearx'] >= curryr - 2) & (temp['d_totavailx'] < 0), 0, temp['non_surv_flag'])
    temp['non_surv_flag'] = np.where((temp['non_surv_flag'] == 1) & (temp['l_ind_avail'].isnull() == False) & (temp['yearx'] >= curryr - 2) & (temp['d_totavailx'] < 0) & (temp['totavailx'] < temp['l_ind_avail']), 0, temp['non_surv_flag'])
    temp= temp[temp['non_surv_flag'] > 0]
    temp = temp.drop_duplicates('msq_id')
    temp = temp[['msq_id', 'non_surv_flag']]
    msq_data = msq_data.join(temp.set_index('msq_id'), on='msq_id')
    msq_data['non_surv_flag'] = msq_data['non_surv_flag'].fillna(0)

    msq_data['count_survs'] = msq_data[msq_data['availxM'] == 0].groupby('msq_id')['msq_id'].transform('count')
    msq_data['count_survs'] = msq_data.groupby('msq_id')['count_survs'].ffill()

    msq_data = msq_data[(msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon)]

    msq_data['all_abs_flag'] = np.where((msq_data['c_ind_avail'].isnull() == False) & (msq_data['d_ind_avail'].isnull() == False) & ((abs(msq_data['d_totavailx'] - msq_data['d_ind_avail']) >= 1000) | (msq_data['d_totavailx'] * msq_data['d_ind_avail'] < 0)), 1, 0)

    msq_data['only_surv_flag'] = np.where((msq_data['count_survs'] == 1) & (msq_data['d_totavailx'] != 0) & (msq_data['d_totavailx'].isnull() == False) & (msq_data['availxM'] == 0) & (msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon), 1, 0)

    msq_data['small_abs_flag'] = np.where((abs(msq_data['d_totavailx']) < 2000) & (msq_data['d_totavailx'] != 0) & (msq_data['type2'] == "F"), 1, 0)
    msq_data['small_abs_flag'] = np.where((abs(msq_data['d_totavailx']) < 20000) & (msq_data['d_totavailx'] != 0) & (msq_data['sizex'] >= 20000) & (msq_data['type2'] == "DW"), 1, msq_data['small_abs_flag'])

    msq_data_filt = msq_data.copy()
    msq_data_filt = msq_data_filt[(msq_data_filt['non_surv_flag'] == 1) | (msq_data_filt['all_abs_flag'] == 1) | (msq_data_filt['only_surv_flag'] == 1) | (msq_data_filt['small_abs_flag'] == 1)]

    msq_data_filt = msq_data_filt[['identity', 'msq_id', 'metcode', 'subid', 'type2', 'yearx', 'month', 'sizex', 'availx', 'totavailx', 'vacratx', 'totvacx', 'availxM', 'sublet', 'subletxM', 'd_totavailx', 'd_ind_avail', 'non_surv_flag', 'all_abs_flag', 'only_surv_flag', 'small_abs_flag']]

    msq_data_filt = msq_data_filt.rename(columns={'msq_id': 'id'})

    flag_cols = ['non_surv_flag', 'all_abs_flag', 'only_surv_flag', 'small_abs_flag']

    msq_data_filt['flag_period'] = str(currmon) + "/" + str(curryr)

    msq_data_filt.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/vac_g.pickle'.format(sector_val, curryr, currmon))

    return flag_cols

