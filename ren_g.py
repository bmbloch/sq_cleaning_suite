import pandas as pd
import numpy as np
from datetime import datetime
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.4f}'.format
from IPython.core.display import display, HTML

def ren_g_flags(sector_val, curryr, currmon, msq_data_in):

    currqtr = int(np.ceil(currmon / 3))

    msq_data = msq_data_in.copy()
    msq_data = msq_data.rename(columns={'id': 'msq_id', 'realid': 'id'})

    msq_data.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)

    msq_data['d_renx'] = np.where((msq_data['id'] == msq_data['id'].shift(1)), (msq_data['renx'] - msq_data['renx'].shift(1)) / msq_data['renx'].shift(1), np.nan)
    msq_data['d_renx'] = round(msq_data['d_renx'], 3)

    temp = msq_data.copy()
    temp['identity_period'] = temp['msq_id'].astype(str) + temp['yr'].astype(str) + temp['qtr'].astype(str) + temp['currmon'].astype(str)

    has_currsurv = msq_data.copy()
    has_currsurv = has_currsurv[(has_currsurv['renxM'] == 0) & (has_currsurv['yr'] == curryr) & (has_currsurv['currmon'] == currmon)]
    has_currsurv = has_currsurv[['msq_id']]
    has_currsurv['has_currsurv'] = 1
    temp = temp.join(has_currsurv.set_index('msq_id'), on='msq_id')
    temp = temp[temp['has_currsurv'] == 1]

    temp['count_survs'] = temp[temp['renxM'] == 0].groupby('msq_id')['msq_id'].transform('count')
    temp['count_survs'] = temp.groupby(['msq_id'])['count_survs'].bfill()
    temp = temp[temp['count_survs'] >= 2]

    temp.sort_values(by=['msq_id', 'yr', 'qtr', 'currmon'], ascending=[True, False, False, False], inplace=True)
    count_rows = pd.DataFrame(temp[temp['renxM'] == 0].set_index('identity_period').groupby('msq_id').cumcount())
    count_rows.columns = ['period']
    temp = temp.join(count_rows, on='identity_period')
    temp['period'] = temp.groupby(['msq_id'])['period'].bfill()
    temp = temp[temp['period'] <= 1]
    temp['count_total_periods'] = temp.groupby('msq_id')['period'].transform('count')
    temp['mondiff'] = temp['count_total_periods'] - 1

    temp.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
    monthized = temp.copy()
    monthized = monthized[monthized['renxM'] == 0]
    monthized['level_diff_total'] = np.where(monthized['msq_id'] == monthized['msq_id'].shift(1), monthized['renx'] - monthized['renx'].shift(1), np.nan)
    monthized['level_diff_monthized'] = monthized['level_diff_total'] / monthized['mondiff']
    monthized = monthized[(monthized['yr'] == curryr) & (monthized['currmon'] == currmon)]
    monthized = monthized[['msq_id', 'level_diff_monthized', 'level_diff_total']]
    temp = temp.join(monthized.set_index('msq_id'), on='msq_id')

    temp['level_diff'] = np.where((temp['id'] == temp['id'].shift(1)), temp['renx'] - temp['renx'].shift(1), np.nan)
    temp['diff_to_monthized'] = temp['level_diff'] - temp['level_diff_monthized']
    temp['diff_pct_total'] = temp['diff_to_monthized'] / temp['level_diff_total']
    temp['smooth_flag'] = np.where(((abs(temp['diff_pct_total']) > 0.10) & (abs(temp['level_diff_total']) * 100 > temp['mondiff']) & (abs(temp['diff_to_monthized']) > 0.01)) | (temp['level_diff'] * temp['level_diff_monthized'] < 0), 1, 0)
    temp = temp[temp['smooth_flag'] == 1]
    temp = temp.drop_duplicates('msq_id')
    temp = temp[['msq_id', 'smooth_flag', 'mondiff']]

    msq_data = msq_data.join(temp.set_index('msq_id'), on='msq_id')
    msq_data['smooth_flag'] = msq_data['smooth_flag'].fillna(0)
    
    opex_l_rsurv = msq_data.copy()
    opex_l_rsurv = opex_l_rsurv[opex_l_rsurv['renxM'] == 0]
    opex_l_rsurv = opex_l_rsurv[(opex_l_rsurv['yr'] != curryr) | ((opex_l_rsurv['yr'] == curryr) & (opex_l_rsurv['currmon'] != currmon))]
    opex_l_rsurv.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, False, False, False], inplace=True)
    opex_l_rsurv = opex_l_rsurv.drop_duplicates('msq_id')
    opex_l_rsurv = opex_l_rsurv[['msq_id', 'opex', 'renx']]
    opex_l_rsurv = opex_l_rsurv.rename(columns={'opex': 'mr_opex_rsurv', 'renx': 'l_renx_sqnet'})
    msq_data = msq_data.join(opex_l_rsurv.set_index('msq_id'), on='msq_id')

    msq_data = msq_data[(msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon)]

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
    msq_data.sort_values(by=['identity'], ascending=[True], inplace=True)

    last_rent = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/last_rent.pickle'.format(sector_val, curryr, currmon))
    last_rent = last_rent.rename(columns={'l_renx': 'l_renx_s', 'renx': 'renx_s'})
    msq_data = msq_data.join(last_rent.set_index('id'), on='id')

    metro_avgs = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/metro_avgs.pickle'.format(sector_val, curryr, currmon))
    metro_avgs = metro_avgs.rename(columns={'uniq2': 'identity_met', 'avg_cat_curr_G_renxAdj_met': 'met_g_rent', 'stdev_cat_curr_G_renxAdj_met': 'met_sd_rent', 'count_cat_curr_G_renxAdj_met': 'met_count_rent', 
                                    'count_curr_ren_met_up': 'met_count_up', 'count_curr_ren_met_down': 'met_count_down', 'count_curr_ren_met_zero': 'met_count_zero'})
    metro_avgs = metro_avgs[['identity_met', 'met_g_rent', 'met_sd_rent', 'met_count_rent', 'met_count_up', 'met_count_down', 'met_count_zero']]
    msq_data = msq_data.join(metro_avgs.set_index('identity_met'), on='identity_met')

    sub_avgs = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sub_avgs.pickle'.format(sector_val, curryr, currmon))
    sub_avgs = sub_avgs.rename(columns={'uniq1': 'identity', 'avg_cat_curr_G_renxAdj_sub': 'sub_g_rent', 'stdev_cat_curr_G_renxAdj_sub': 'sub_sd_rent', 'count_cat_curr_G_renxAdj_sub': 'sub_count_rent',
                                        'count_curr_ren_sub_up': 'sub_count_up', 'count_curr_ren_sub_down': 'sub_count_down', 'count_curr_ren_sub_zero': 'sub_count_zero'})
    sub_avgs = sub_avgs.drop(['type2', 'metcode', 'subid'], axis=1)
    msq_data = msq_data.join(sub_avgs.set_index('identity'), on='identity')

    us_avgs = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/us_avgs_notier.pickle'.format(sector_val, curryr, currmon))
    us_avgs = us_avgs[['type2', 'avg_cat_curr_G_renxAdj_us_notier']]
    us_avgs = us_avgs.rename(columns={'avg_cat_curr_G_renxAdj_us_notier': 'us_g_rent'})
    msq_data = msq_data.join(us_avgs.set_index('type2'), on='type2')

    msq_data['sub_g_rent'] = round(msq_data['sub_g_rent'], 3)
    msq_data['met_g_rent'] = round(msq_data['met_g_rent'], 3)
    msq_data['us_g_rent'] = round(msq_data['us_g_rent'], 3)

    def gen_reis_survdate(msq_data, survdate_var, prefix, splitter):

        data = msq_data.copy()
        data = data[['msq_id', survdate_var]]
        data = data[data[survdate_var].isnull() == False]
        data[survdate_var] = data[survdate_var].astype(str)
        data[[prefix + 'suryr', prefix + 'surmon', prefix + 'surday']] = data[survdate_var].str.split(splitter,expand=True)
        data[prefix + 'surmon'] = np.where(data[prefix + 'surmon'] == '', np.nan, data[prefix + 'surmon'])
        data[prefix + 'surday'] = np.where(data[prefix + 'surday'] == None, np.nan, data[prefix + 'surday'])
        data[prefix + 'suryr'] = np.where(data[prefix + 'suryr'] == None, np.nan, data[prefix + 'suryr'])
        data[[prefix + 'surmon', prefix + 'surday', prefix + 'suryr']] = data[[prefix + 'surmon', prefix + 'surday', prefix + 'suryr']].astype(float)
        data[prefix + 'reismon'] = np.where((data[prefix + 'surday'] <= 15), data[prefix + 'surmon'], data[prefix + 'surmon'] + 1)
        data[prefix + 'reismon'] = np.where((data[prefix + 'surday'] > 15) & (data[prefix + 'surmon'] == 12), 1, data[prefix + 'reismon'])
        data[prefix + 'reisyr'] = np.where((data[prefix + 'surmon'] != 12), data[prefix + 'suryr'], data[prefix + 'suryr'] + 1)
        data[prefix + 'reisyr'] = np.where((data[prefix + 'surmon'] == 12) & (data[prefix + 'surday'] <= 15), data[prefix + 'suryr'], data[prefix + 'reisyr'])
        data[prefix + 'reisqtr'] = np.where(data[prefix + 'reismon'] <= 3, 1, 4)
        data[prefix + 'reisqtr'] = np.where((data[prefix + 'reismon'] > 3) & (data[prefix + 'reismon'] <= 6), 2, data[prefix + 'reisqtr'])
        data[prefix + 'reisqtr'] = np.where((data[prefix + 'reismon'] > 6) & (data[prefix + 'reismon'] <= 9), 3, data[prefix + 'reisqtr'])

        msq_data = msq_data.drop([survdate_var], axis=1)
        msq_data = msq_data.join(data.set_index('msq_id'), on='msq_id')

        return msq_data
    
    msq_data = gen_reis_survdate(msq_data, 'mr_survdate', 'mr_', "-")
    msq_data = gen_reis_survdate(msq_data, 'l_survdate', 'l_', "-")

    msq_data[['sub_count_rent', 'sub_count_up', 'sub_count_down', 'sub_count_zero', 'met_count_rent', 'met_count_up', 'met_count_down', 'met_count_zero']] = msq_data[['sub_count_rent', 'sub_count_up', 'sub_count_down', 'sub_count_zero', 'met_count_rent', 'met_count_up', 'met_count_down', 'met_count_zero']].fillna(0)

    msq_data['mr_survdate'] = pd.to_datetime(msq_data['mr_survdate'])
    msq_data['l_survdate'] = pd.to_datetime(msq_data['l_survdate'])
    start = datetime(curryr, currmon, 1)
    msq_data['mr_diff_mon'] = (start.year - msq_data['mr_survdate'].dt.year) * 12 + (start.month - msq_data['mr_survdate'].dt.month)
    msq_data['l_diff_mon'] = (start.year - msq_data['l_survdate'].dt.year) * 12 + (start.month - msq_data['l_survdate'].dt.month)
    
    msq_data['recent_flag'] = np.where(
                                        (msq_data['mr_diff_mon'] <= 3) & 
                                        (
                                            ((abs(msq_data['mr_G_avgrenx']) > 0.003) & (abs(msq_data['d_renx']) > 0.003)) | 
                                            ((round(msq_data['mr_G_avgrenx'],3) * round(msq_data['d_renx'],3) < 0) & ((abs(msq_data['mr_G_avgrenx']) >= 0.002) | (abs(msq_data['d_renx']) >= 0.002))) | 
                                            ((abs(msq_data['mr_G_avgrenx']) > 0.015) & (abs(msq_data['d_renx']) > 0) & (msq_data['mr_diff_mon'] == 1))
                                        ) & 
                                        (msq_data['mr_G_avgrenx'].isnull() == False) &
                                        (msq_data['d_renx'] != 0) & 
                                        (msq_data['G_avgrenxAdj'].isnull() == True), 
                                        1, 0)

    msq_data['opex_level_diff'] = msq_data['opex'] - msq_data['mr_opex_rsurv']
    msq_data['renx_level_diff'] = msq_data['renx'] - msq_data['l_renx_sqnet']
    msq_data['opex_level_flag'] = np.where(
                                            (msq_data['G_avgrenxAdj'] == 0) & 
                                            (msq_data['d_renx'] > 0) & 
                                            (msq_data['opex_level_diff'] < 0) & 
                                            (abs(msq_data['opex_level_diff']) / abs(msq_data['renx_level_diff']) >= 0.25), 
                                            1, 0)
    msq_data['opex_level_flag'] = np.where(
                                            (msq_data['G_avgrenxAdj'] == 0) & 
                                            (msq_data['d_renx'] < 0) & 
                                            (msq_data['opex_level_diff'] > 0) & 
                                            (abs(msq_data['opex_level_diff']) / abs(msq_data['renx_level_diff']) >= 0.25), 
                                            1, msq_data['opex_level_flag'])

    msq_data['g_diff'] = msq_data['d_renx'] - msq_data['G_avgrenxAdj']
    msq_data['g_diff_flag'] = np.where(
                                        (msq_data['opex_level_flag'] == 0) & 
                                        (msq_data['smooth_flag'] == 0) & 
                                        (msq_data['g_diff'].isnull() == False) &
                                        (
                                          ((msq_data['d_renx'] < (msq_data['G_avgrenxAdj'] * 0.8)) & (abs(msq_data['g_diff']) > 0.002) & (msq_data['G_avgrenxAdj'] > 0) & (msq_data['d_renx'] < 0.08)) |
                                          ((msq_data['d_renx'] > msq_data['G_avgrenxAdj'] + 0.1) & (msq_data['G_avgrenxAdj'] > 0)) | 
                                          ((msq_data['d_renx'] > (msq_data['G_avgrenxAdj'] * 0.8)) & (abs(msq_data['g_diff']) > 0.002) & (msq_data['G_avgrenxAdj'] < 0) & (msq_data['d_renx'] > -0.08)) |
                                          ((msq_data['d_renx'] < msq_data['G_avgrenxAdj'] - 0.1) & (msq_data['G_avgrenxAdj'] < 0)) | 
                                          (abs(msq_data['d_renx']) > abs(msq_data['G_avgrenx'])) | 
                                          ((msq_data['d_renx'] != 0) & (msq_data['G_avgrenxAdj'] == 0)) | 
                                          ((msq_data['d_renx'] == 0) & (msq_data['G_avgrenxAdj'] != 0)) | 
                                          ((msq_data['d_renx'] > 0) & (msq_data['G_avgrenxAdj'] < 0)) | 
                                          ((msq_data['d_renx'] < 0) & (msq_data['G_avgrenxAdj'] > 0))
                                        ), 1, 0)

    msq_data['g_diff_flag'] = np.where(
                                        (msq_data['g_diff_flag'] == 1) & 
                                        (msq_data['l_diff_mon'] > 24) & 
                                        (abs(msq_data['d_renx']) < abs(msq_data['G_avgrenx'])) &
                                        (msq_data['d_renx'] * msq_data['G_avgrenx'] >= 0) & 
                                        (abs(msq_data['G_avgrenx']) <= 0.01),
                                        0, msq_data['g_diff_flag'])

    msq_data['sub_diff'] = msq_data['d_renx'] - msq_data['sub_g_rent']
    msq_data['met_diff'] = msq_data['d_renx'] - msq_data['met_g_rent']

    msq_data['total_sub_props'] = msq_data.groupby('identity')['msq_id'].transform('count')
    msq_data['total_met_props'] = msq_data.groupby('identity_met')['msq_id'].transform('count')

    msq_data['sub_above_thresh'] = np.where(((msq_data['total_sub_props'] > 10) & (msq_data['sub_count_rent'] >= 4) & (msq_data['sub_count_rent'] / msq_data['total_sub_props'] >= 0.1)) | ((msq_data['total_sub_props'] <= 10) & (msq_data['sub_count_rent'] / msq_data['total_sub_props'] >= 0.25)), 1, 0)
    msq_data['met_above_thresh'] = np.where(((msq_data['total_met_props'] > 10) & (msq_data['met_count_rent'] >= 4) & (msq_data['met_count_rent'] / msq_data['total_met_props'] >= 0.05)) | ((msq_data['total_met_props'] <= 10) & (msq_data['met_count_rent'] / msq_data['total_met_props'] >= 0.25)), 1, 0)
    msq_data['sub_down_above_thresh'] = np.where(((msq_data['total_sub_props'] > 10) & (msq_data['sub_count_down'] >= 4) & (msq_data['sub_count_down'] / msq_data['total_sub_props'] >= 0.1)) | ((msq_data['total_sub_props'] <= 10) & (msq_data['sub_count_down'] / msq_data['total_sub_props'] >= 0.25)), 1, 0)
    msq_data['met_down_above_thresh'] = np.where(((msq_data['total_met_props'] > 10) & (msq_data['met_count_down'] >= 4) & (msq_data['met_count_down'] / msq_data['total_met_props'] >= 0.05)) | ((msq_data['total_met_props'] <= 10) & (msq_data['met_count_down'] / msq_data['total_met_props'] >= 0.25)), 1, 0)
    msq_data['sub_up_above_thresh'] = np.where(((msq_data['total_sub_props'] > 10) & (msq_data['sub_count_up'] >= 4) & (msq_data['sub_count_up'] / msq_data['total_sub_props'] >= 0.1)) | ((msq_data['total_sub_props'] <= 10) & (msq_data['sub_count_up'] / msq_data['total_sub_props'] >= 0.25)), 1, 0)
    msq_data['met_up_above_thresh'] = np.where(((msq_data['total_met_props'] > 10) & (msq_data['met_count_up'] >= 4) & (msq_data['met_count_up'] / msq_data['total_met_props'] >= 0.05)) | ((msq_data['total_met_props'] <= 10) & (msq_data['met_count_up'] / msq_data['total_met_props'] >= 0.25)), 1, 0)
    
    msq_data['sub_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_g_rent'].isnull() == False) & 
                                    (msq_data['sub_above_thresh'] == 1) & 
                                    (
                                        ((abs(msq_data['d_renx']) < abs(msq_data['sub_g_rent']) * 0.5) & ((abs(msq_data['d_renx'] - msq_data['sub_g_rent']) > 0.002) | (msq_data['d_renx'] == 0))) | 
                                        ((abs(msq_data['d_renx']) > abs(msq_data['sub_g_rent']) * 2) & ((abs(msq_data['d_renx'] - msq_data['sub_g_rent']) > 0.002) | (msq_data['d_renx'] == 0))) |
                                        (msq_data['d_renx'] * msq_data['sub_g_rent'] < 0)
                                    ), 1, 0)

    msq_data['sub_cat'] = np.where(
                                    (msq_data['sub_cat'] == 1) &
                                    (msq_data['sub_g_rent'] < 0) & 
                                    (msq_data['sub_count_down'] / msq_data['total_sub_props'] < 0.5) & 
                                    ((msq_data['d_renx'] == 0) | ((msq_data['d_renx'] > msq_data['sub_g_rent']) & (msq_data['d_renx'] < 0)))
                                    , 0, msq_data['sub_cat'])

    msq_data['sub_cat'] = np.where(
                                    (msq_data['sub_cat'] == 1) &
                                    (msq_data['sub_g_rent'] < 0) & 
                                    (msq_data['sub_count_down'] / msq_data['total_sub_props'] < 0.2) & 
                                    (msq_data['d_renx'] <= msq_data['us_g_rent'])
                                , 0, msq_data['sub_cat'])
    
    msq_data['sub_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['sub_down_above_thresh'] == 1) & 
                                    (msq_data['sub_g_rent'] <= 0) & 
                                    (msq_data['d_renx'] >= 0.02) & 
                                    (msq_data['sub_count_down'] > msq_data['sub_count_up']), 
                                    3, msq_data['sub_cat'])

    msq_data['sub_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['sub_g_rent'] >= 0) & (msq_data['d_renx'] < 0) & 
                                    (msq_data['sub_count_up'] > msq_data['sub_count_down']) &
                                    (msq_data['sub_up_above_thresh'] == 1), 
                                    4, msq_data['sub_cat'])

    msq_data['met_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['met_g_rent'].isnull() == False) & 
                                    (msq_data['sub_above_thresh'] == 0) & 
                                    (msq_data['met_above_thresh'] == 1) & 
                                    (
                                        ((abs(msq_data['d_renx']) < abs(msq_data['met_g_rent']) * 0.5) & ((abs(msq_data['d_renx'] - msq_data['met_g_rent']) > 0.002) | (msq_data['d_renx'] == 0))) | 
                                        ((abs(msq_data['d_renx']) > abs(msq_data['met_g_rent']) * 2) & ((abs(msq_data['d_renx'] - msq_data['met_g_rent']) > 0.002) | (msq_data['d_renx'] == 0))) |
                                        (msq_data['d_renx'] * msq_data['met_g_rent'] < 0)
                                    ), 
                                    1, 0)

    msq_data['met_cat'] = np.where(
                                    (msq_data['met_cat'] == 1) &
                                    (msq_data['met_g_rent'] < 0) & 
                                    (msq_data['met_count_down'] / msq_data['total_met_props'] < 0.2) & 
                                    ((msq_data['d_renx'] == 0) | ((msq_data['d_renx'] > msq_data['met_g_rent']) & (msq_data['d_renx'] < 0)))
                                    , 0, msq_data['met_cat'])

    msq_data['met_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & (msq_data['met_cat'] == 0) & 
                                    (abs(msq_data['d_renx']) >= 0.03) & 
                                    ((msq_data['met_g_rent'].isnull() == True) | ((msq_data['met_above_thresh'] == 0) & (msq_data['sub_above_thresh'] == 0))) & 
                                    (msq_data['d_renx'].isnull() == False), 
                                    2, msq_data['met_cat'])

    msq_data['met_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['met_cat'] == 0) & 
                                    (msq_data['met_g_rent'] <= 0) & 
                                    (msq_data['d_renx'] >= 0.02) & 
                                    (msq_data['met_count_down'] > msq_data['met_count_up']) &
                                    (msq_data['met_down_above_thresh'] == 1) & 
                                    (msq_data['sub_up_above_thresh'] == 0), 
                                    3, msq_data['met_cat'])

    msq_data['met_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['met_cat'] == 0) & (msq_data['met_g_rent'] >= 0) & 
                                    (msq_data['d_renx'] < 0) & (msq_data['met_count_up'] > msq_data['met_count_down']) &
                                    (msq_data['met_up_above_thresh'] == 1) & 
                                    (msq_data['sub_down_above_thresh'] == 0), 
                                    4, msq_data['met_cat'])

    msq_data['us_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & (msq_data['met_cat'] == 0) & 
                                    (msq_data['sub_above_thresh'] == 0) & 
                                    (msq_data['met_above_thresh'] == 0) & 
                                    (msq_data['d_renx'] * msq_data['us_g_rent'] < 0), 
                                    1, 0) 
    
    msq_data['us_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['met_cat'] == 0) & 
                                    (msq_data['us_cat'] == 0) & 
                                    (msq_data['sub_above_thresh'] == 0) & 
                                    (msq_data['met_above_thresh'] == 0) & 
                                    (msq_data['d_renx'] > 0) & 
                                    (msq_data['d_renx'] > msq_data['us_g_rent'] * 2), 
                                    2, msq_data['us_cat']) 
    
    msq_data['us_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['met_cat'] == 0) & 
                                    (msq_data['us_cat'] == 0) & 
                                    (msq_data['sub_above_thresh'] == 0) & 
                                    (msq_data['met_above_thresh'] == 0) & 
                                    (msq_data['d_renx'] < 0) & 
                                    (msq_data['d_renx'] < msq_data['us_g_rent'] * 2), 
                                    3, msq_data['us_cat'])

    msq_data['us_cat'] = np.where(
                                    (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                    (msq_data['sub_cat'] == 0) & 
                                    (msq_data['met_cat'] == 0) &
                                    (msq_data['us_cat'] == 0) &  
                                    (msq_data['sub_above_thresh'] == 0) & 
                                    (msq_data['met_above_thresh'] == 0) & 
                                    (msq_data['us_g_rent'] > 0) & 
                                    (msq_data['d_renx'] >= 0) & 
                                    (msq_data['d_renx'] < msq_data['us_g_rent'] - 0.005), 
                                    4, msq_data['us_cat']) 

    msq_data['us_cat'] = np.where(
                                  (msq_data['G_avgrenxAdj'].isnull() == True) & 
                                  (msq_data['sub_cat'] == 0) & 
                                  (msq_data['met_cat'] == 0) & 
                                  (msq_data['us_cat'] == 0) & 
                                  (msq_data['sub_above_thresh'] == 0) & 
                                  (msq_data['met_above_thresh'] == 0) &
                                  (msq_data['d_renx'] == 0) & 
                                  (msq_data['mr_diff_mon'] > 3) &
                                  (abs(msq_data['us_g_rent']) > 0), 
                                  5, msq_data['us_cat'])

   
    msq_data['us_cat'] = np.where(
                                    (msq_data['us_cat'] != 0) & 
                                    (msq_data['us_cat'] < 5) &
                                    (msq_data['d_renx'] < 0) & 
                                    (msq_data['us_g_rent'] > 0) & 
                                    (msq_data['met_count_down'] / msq_data['met_count_rent'] >= 0.30) &
                                    (msq_data['d_renx'] >= msq_data['met_g_rent'] - 0.002) & 
                                    (msq_data['d_renx'] * msq_data['met_g_rent'] >= 0) & 
                                    (msq_data['met_count_rent'] / msq_data['total_met_props'] >= 0.05),  
                                    0, msq_data['us_cat'])
    
    msq_data['us_cat'] = np.where(
                                    (msq_data['us_cat'] != 0) & 
                                    (msq_data['us_cat'] < 5) &
                                    (msq_data['d_renx'] > 0) & 
                                    (msq_data['us_g_rent'] < 0) & 
                                    (msq_data['met_count_up'] / msq_data['met_count_rent'] >= 0.30) &
                                    (msq_data['d_renx'] <= msq_data['met_g_rent'] + 0.002) & 
                                    (msq_data['d_renx'] * msq_data['met_g_rent'] >= 0) & 
                                    (msq_data['met_count_rent'] / msq_data['total_met_props'] >= 0.05),  
                                    0, msq_data['us_cat'])

    
    msq_data['sub_sqg_flag'] = np.where(msq_data['sub_cat'] > 0, 1, 0)
    msq_data['met_sqg_flag'] = np.where(msq_data['met_cat'] > 0, 1, 0)
    msq_data['us_sqg_flag'] = np.where(msq_data['us_cat'] > 0, 1, 0)
    msq_data['sqg_flag'] = np.where((msq_data['sub_sqg_flag'] == 1) | (msq_data['met_sqg_flag'] == 1) | (msq_data['us_sqg_flag'] == 1), 1, 0)

    msq_data['nc_tag'] = np.where((msq_data['yearx'] == curryr) & (currmon - msq_data['month'] < 3) & (currmon >= 3), 1, 0)
    msq_data['nc_tag'] = np.where((msq_data['yearx'] == curryr - 1) & (currmon == 2) & (msq_data['month'] == 12), 1, msq_data['nc_tag'])
    msq_data['nc_tag'] = np.where((msq_data['yearx'] == curryr - 1) & (currmon == 1) & (msq_data['month'] == 11), 1, msq_data['nc_tag'])
    
    msq_data['nc_flag'] = np.where(
                                    (msq_data['nc_tag'] == 1) & 
                                    ((msq_data['d_renx'] > 0.003) | (msq_data['d_renx'] < 0)) & 
                                    (msq_data['renxM'] == 1), 
                                    1, 0)

    msq_data['smooth_flag'] = np.where(
                                        (msq_data['smooth_flag']== 1) & 
                                        (msq_data['d_renx'] / msq_data['G_avgrenx'] >= 0.4) & 
                                        (msq_data['d_renx'] / msq_data['G_avgrenx'] <= 0.75) &
                                        (msq_data['mondiff'] <= 12) & (msq_data['mondiff'] > 3) &
                                        (msq_data['d_renx'] * msq_data['G_avgrenx'] > 0), 
                                        0, msq_data['smooth_flag'])


    msq_data['has_flag'] = np.where((msq_data['recent_flag'] == 1) | (msq_data['opex_level_flag']) | (msq_data['g_diff_flag'] == 1) | (msq_data['sqg_flag'] == 1) | (msq_data['nc_flag'] == 1) | (msq_data['smooth_flag'] == 1), 1, 0)
    
    msq_data = msq_data[msq_data['has_flag'] == 1]

    msq_data = msq_data[['identity', 'msq_id', 'metcode', 'subid', 'type2', 'yearx', 'month', 'renx', 'renxM', 'd_renx', 'G_avgrenx', 'G_avgrenxAdj', 'g_diff', 'sub_g_rent', 'met_g_rent', 'us_g_rent', 'l_renx_s', 'renx_s', 'survdate', 'l_survdate', 'mr_survdate', 'mr_G_avgrenx', 
                        'total_sub_props', 'total_met_props', 'sub_count_rent', 'sub_count_up', 'sub_count_down', 'sub_count_zero', 'met_count_rent', 'met_count_up', 'met_count_down', 'met_count_zero', 'sub_cat', 'met_cat', 'us_cat', 'opex', 'mr_opex_rsurv', 'opex_level_diff', 
                        'recent_flag', 'opex_level_flag', 'g_diff_flag', 'sqg_flag', 'nc_flag', 'smooth_flag']]

    msq_data = msq_data.rename(columns={'msq_id': 'id'})

    msq_data['flag_period'] = str(currmon) + "/" + str(curryr)
    
    msq_data.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/ren_g.pickle'.format(sector_val, curryr, currmon))

    flag_cols = ['recent_flag', 'opex_level_flag', 'g_diff_flag', 'sqg_flag', 'nc_flag', 'smooth_flag']

    return flag_cols