import pandas as pd
import numpy as np
from datetime import datetime
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.2f}'.format
from IPython.core.display import display, HTML

def outlier_r_level_flags(sector_val, curryr, currmon, msq_data_in, past_msq_data_in):

    msq_data = msq_data_in.copy()
    msq_data = msq_data.rename(columns={'id': 'msq_id', 'realid': 'id'})
    msq_data = msq_data[(msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon)]

    msq_data['type2'] = np.where(msq_data['type2'] == "F", "F", "DW")

    expansion_list = ["AA", "AB", "AK", "AN", "AQ", "BD", "BF", "BI", "BR", "BS", "CG", "CM", "CN", "CS", "DC", 
                    "DM", "DN", "EP", "FC", "FM", "FR", "GD", "GN", "GR", "HR", "HL", "HT", "KX", "LL", "LO", 
                    "LR", "LV", "LX", "MW", "NF", "NM", "NO", "NY", "OK", "OM", "PV", "RE", "RO", "SC", "SH", 
                    "SR", "SS", "ST", "SY", "TC", "TM", "TO", "TU", "VJ", "VN", "WC", "WK", "WL", "WS"]

    msq_data['expansion'] = np.where(msq_data['metcode'].isin(expansion_list), "Yes", "No")
    msq_data = msq_data[(msq_data['expansion'] == "No") | (msq_data['type2'] == "DW")]

    msq_data['msq_id'] = msq_data['msq_id'].astype(int)
    msq_data['uniq1'] = msq_data['metcode'] + msq_data['subid'].astype(str) + msq_data['type2']
    msq_data['uniq2'] = msq_data['metcode'] + msq_data['type2']
    msq_data['sub_live'] = np.where(msq_data['submkt'].str[0:2] == "99", 0, 1)
    msq_data = msq_data[msq_data['sub_live'] == 1]
    msq_data.sort_values(by=['uniq1'], ascending=[True], inplace=True)

    for perc in [0.8, 0.2, 0.65, 0.90]:
        percentile = msq_data.copy()
        percentile = pd.DataFrame(percentile.groupby('uniq1')['renx'].quantile(perc))
        percentile.columns = ['per' + str(int(perc * 100))]
        percentile['per' + str(int(perc * 100))] = round(percentile['per' + str(int(perc * 100))], 2)
        msq_data = msq_data.join(percentile, on='uniq1')

    past_msq_data = past_msq_data_in.copy()

    past_msq_data = past_msq_data.rename(columns={'id': 'msq_id'})
    past_msq_data = past_msq_data.drop_duplicates('msq_id')
    past_msq_data = past_msq_data[past_msq_data['yearx'] >= curryr - 3]
    past_msq_data['nc_new_tag'] = 0
    past_msq_data = past_msq_data.set_index('msq_id')
    past_msq_data = past_msq_data[['nc_new_tag']]

    msq_data = msq_data.join(past_msq_data, on='msq_id')
    msq_data['nc_new_tag'] = np.where((msq_data['nc_new_tag'].isnull() == True) & (msq_data['yearx'] >= curryr - 3), 1, 0)

    mr_rent = msq_data_in.copy()
    mr_rent = mr_rent.rename(columns={'id': 'msq_id', 'realid': 'id'})
    mr_rent = mr_rent[mr_rent['renxM'] == 0]
    mr_rent.sort_values(by=['yr', 'qtr', 'currmon'], ascending=[False, False, False], inplace=True)
    mr_rent = mr_rent.drop_duplicates('msq_id')
    mr_rent = mr_rent.rename(columns= {'renx': 'mr_rent'})
    mr_rent = mr_rent.set_index('msq_id')
    mr_rent = mr_rent[['mr_rent']]

    msq_data = msq_data.join(mr_rent, on='msq_id')

    metro_avgs = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/metro_avgs.pickle'.format(sector_val, curryr, currmon))
    sub_levels = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sub_levels.pickle'.format(sector_val, curryr, currmon))
    last_rent = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/last_rent.pickle'.format(sector_val, curryr, currmon))
    last_rent = last_rent.drop(['renx', 'uniq1'], axis=1)

    msq_data = msq_data.join(metro_avgs.set_index('uniq2'), on='uniq2')
    msq_data = msq_data.join(sub_levels.set_index('uniq1'), on='uniq1')
    msq_data = msq_data.join(last_rent.set_index('id'), on='id')

    msq_data['renx_unnet'] = np.where(msq_data['rnt_term'] == "G", msq_data['renx'] + msq_data['opex'], msq_data['renx'])
    msq_data['l_renx'] = np.where((msq_data['l_rnt_term'] == "G") & (msq_data['rnt_term'] != "G"), msq_data['l_renx'] - msq_data['opex'], msq_data['l_renx'])
    msq_data['l_unnet_tag'] = np.where((msq_data['l_rnt_term'] == "G") & (msq_data['rnt_term'] != "G"), 1, 0)
    msq_data['chg_last_quote'] = (msq_data['renx_unnet'] - msq_data['l_renx']) / msq_data['l_renx']
    msq_data['l_survdate']= pd.to_datetime(msq_data['l_survdate'])
    start = datetime(curryr, currmon, 1)
    msq_data['l_mon_diff'] = (start.year - msq_data['l_survdate'].dt.year) * 12 + (start.month - msq_data['l_survdate'].dt.month)
    msq_data['chg_monthized'] = msq_data['chg_last_quote'] / msq_data['l_mon_diff']
    msq_data['chg_monthized'] = round(msq_data['chg_monthized'], 3)

    mr_term = msq_data_in.copy()
    mr_term = mr_term.rename(columns={'id': 'msq_id', 'realid': 'id'})
    mr_term = mr_term[mr_term['termxM'] == 0]
    mr_term.sort_values(by=['yr', 'qtr', 'currmon'], ascending=[False, False, False], inplace=True)
    mr_term = mr_term.drop_duplicates('msq_id')
    mr_term = mr_term.rename(columns= {'termx': 'mr_term'})
    mr_term = mr_term.set_index('msq_id')
    mr_term = mr_term[['mr_term']]
    msq_data = msq_data.join(mr_term, on='msq_id')

    msq_data['termx_flag'] = np.where((((msq_data['termx'] > 20) & ((msq_data['termx'] > msq_data['mr_term']) | (msq_data['mr_term'].isnull() == True))) | ((msq_data['termx'] < 3) & ((msq_data['termx'] < msq_data['mr_term']) | (msq_data['mr_term'].isnull() == True)))) & (msq_data['termxM'] == 1), 1, 0)

    msq_data['min_max'] = np.where(((msq_data['renx'] < msq_data['per20']) | (msq_data['renx'] > msq_data['per80'])) & (msq_data['l_renx'].isnull() == True) & (msq_data['renxM'] == 1), 1, 0)
    msq_data['min_max'] = np.where((msq_data['renx'] > msq_data['per80']) & (msq_data['yearx'] >= curryr - 1), 0, msq_data['min_max'])

    msq_data['low_g'] = np.where((msq_data['chg_monthized'] < 0.001) & (msq_data['renxM'] == 1) & (msq_data['l_mon_diff'] > 24) & (msq_data['l_survdate'] > '01/01/2010') & (msq_data['l_renx'].isnull() == False)
                                        & (msq_data['renx'] < msq_data['renx_avg']) & (msq_data['renx_avg'].isnull() == False), 1, 0)

    msq_data['sq_aggress'] = np.where(((msq_data['chg_monthized'] < -0.003) | (msq_data['chg_monthized'] > 0.01)) & (msq_data['renxM'] == 1) & (msq_data['chg_monthized'].isnull() == False) & (msq_data['l_mon_diff'] <= 24) & (msq_data['l_renx'].isnull() == False), 1, 0)

    msq_data['nc'] = np.where(((msq_data['yearx'] == curryr) | ((msq_data['yearx'] == curryr - 1) & (msq_data['currmon'] > currmon))) & (msq_data['renx'] < msq_data['per65']) & (msq_data['l_renx'].isnull() == True) & (msq_data['mr_rent'].isnull() == True), 1, 0)
    msq_data['nc'] = np.where((msq_data['nc_new_tag'] == 1) & (msq_data['renx'] < msq_data['per65']) & (msq_data['l_renx'].isnull() == True) & (msq_data['mr_rent'].isnull() == True), 1, msq_data['nc'])
    msq_data['nc'] = np.where((msq_data['nc_new_tag'] == 1) & (msq_data['renx'] >= msq_data['per90']) & (msq_data['mr_rent'].isnull() == True), 1, msq_data['nc'])

    msq_data['found_id'] = "I" + msq_data['id'].astype(str)

    msq_data_filt = msq_data.copy()
    msq_data_filt = msq_data_filt.rename(columns={'uniq1': 'identity'})
    msq_data_filt = msq_data_filt[['identity', 'msq_id', 'found_id', 'metcode', 'subid', 'yearx', 'rnt_term', 'renx', 'renxM', 'termx', 'termxM', 'chg_monthized', 'renx_avg', 'per20', 'per65', 'per80', 'per90', 'nc_new_tag', 'l_mon_diff', 'l_renx', 'min_max', 'low_g', 'sq_aggress', 'nc', 'termx_flag']]
    msq_data_filt = msq_data_filt[(msq_data_filt['min_max'] == 1) | (msq_data_filt['low_g'] == 1) | (msq_data_filt['sq_aggress'] == 1) | (msq_data_filt['nc'] == 1) | (msq_data_filt['termx_flag'] == 1)]

    msq_data_filt = msq_data_filt.rename(columns={'min_max': 'r_flag_min_max', 'low_g': 'r_flag_low_g', 'sq_aggress': 'r_flag_sq_aggress', 'nc': 'r_flag_nc'})
    flag_cols = ['r_flag_min_max', 'r_flag_low_g', 'r_flag_sq_aggress', 'r_flag_nc', 'termx_flag']

    msq_data_filt = msq_data_filt.rename(columns={'msq_id': 'id'})

    msq_data_filt.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/outlier_r_level.pickle'.format(sector_val, curryr, currmon))
   
    return flag_cols