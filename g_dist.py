import pandas as pd
import numpy as np
from datetime import datetime
import scipy.stats as st
import multiprocessing as mp
from tqdm import tqdm
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.4f}'.format
from IPython.core.display import display, HTML

def get_surv_props(surv_data, data, curryr, currmon, identity, sector_val, cov_thresh, l_var):
    data = data.reset_index()
    surv_data_filt = surv_data.copy()

    surv_data_filt = surv_data_filt[surv_data_filt['identity'] == identity]
    surv_count = surv_data_filt['ind_size'].count()

    data_prop = data.copy()
    data_prop = data_prop[data_prop['identity'] == identity]
    prop_count_sub = data_prop['sizex'].count()

    if sector_val == "ind":
        if identity[-2:] == "DW":
            type_filt = "DW"
        else:
            type_filt = "F"
       
    if surv_count / prop_count_sub < cov_thresh:
        surv_data_filt = surv_data.copy()
        surv_data_filt = surv_data_filt.reset_index()
        if sector_val == "ind":
            surv_data_filt = surv_data_filt[(surv_data_filt['metcode'] == identity[0:2]) & (surv_data_filt['type2'] == type_filt)]
        else:
            surv_data_filt = surv_data_filt[surv_data_filt['metcode'] == identity[0:2]]
        surv_data_filt = surv_data_filt[(surv_data_filt['year'] < curryr) | ((surv_data_filt['year'] == curryr) & (surv_data_filt['month'] < currmon))]
        divisor_size = data[(data['metcode'] == identity[0:2]) & (data['type2'] == type_filt)]['sizex'].count()
        surv_count = surv_data_filt['ind_size'].count()

        if surv_count / divisor_size < cov_thresh:
            surv_data_filt = surv_data.copy()
            if sector_val == "ind":
                surv_data_filt = surv_data_filt[(surv_data_filt['type2'] == type_filt)]
            surv_data_filt = surv_data_filt[(surv_data_filt['year'] < curryr) | ((surv_data_filt['year'] == curryr) & (surv_data_filt['month'] < currmon))]
            divisor_size = data[(data['type2'] == type_filt)]['sizex'].count()
            surv_count = surv_data_filt['ind_size'].count()
            level = "Nat"
        else:
            level = "Met"
    else:
        divisor_size = prop_count_sub
        level = "Sub"
    
    return surv_data_filt, level, divisor_size, surv_count

def permutation_test(msq_curr, l_var, g_var, identity, curryr, currmon, sector_val, surv_data, cov_thresh):

    data_filt = msq_curr.copy()
    data_filt = data_filt[data_filt['identity'] == identity]
    data_filt = data_filt[(data_filt['yearx'] < curryr) | ((data_filt['yearx'] == curryr) & (data_filt['month'] < currmon))]
    sq_data = data_filt.copy()
    
    # For the permutation test, only include props not surveyed in the prior two months of the quarter to evaluate the distribution
    if (currmon == 3 or currmon == 6 or currmon == 9 or currmon == 12) and (sector_val == "ind" or sector_val == "ret"):
        sq_data = sq_data[(sq_data[l_var + 'M'] == 1) & (sq_data['has_pq_surv_' + l_var].isnull() == True)]
    else:
        sq_data = sq_data[(sq_data[l_var + 'M'] == 1)]
    
    surv_data, level, divisor_size, surv_count = get_surv_props(surv_data, msq_curr, curryr, currmon, identity, sector_val, cov_thresh, l_var)

    benchmark = surv_data.copy()
    if level == "Sub":
        group_ident = 'identity'
        benchmark = benchmark[benchmark['identity'] == identity]
    if level == "Met":
        group_ident = 'identity_met'
        if identity[-1] == "F":
            benchmark = benchmark[benchmark['identity_met'] == identity[0:2] + "F"]
        else:
            benchmark = benchmark[benchmark['identity_met'] == identity[0:2] + "DW"]
    if level == "Nat":
        group_ident = 'type2'
        if identity[-1] == "F":
            benchmark = benchmark[benchmark['type2'] == "F"]
        else:
            benchmark = benchmark[benchmark['type2'] == "DW"]
    
    benchmark['avg_' + g_var] = benchmark.groupby(group_ident)[g_var].transform('mean')
    
    benchmark = benchmark.drop_duplicates(group_ident)

    benchmark = benchmark.reset_index().loc[0]['avg_' + g_var]
    benchmark = round(benchmark, 3)

    surv_data = list(surv_data[g_var])
    sq_data = list(sq_data[g_var])

    if len(surv_data) == 0 or len(sq_data) == 0:
        p_value = 0
    else:
        surv_data = [round(elem, 3) for elem in surv_data]
        sq_data = [round(elem, 3) for elem in sq_data]
    
        T = np.mean(surv_data) - np.mean(sq_data)
        T = round(T, 3)
        Ts = []
    
        combined = surv_data + sq_data
        rng = np.random.default_rng()
        N = 10000
        resamp = np.array([rng.choice(combined, size=len(combined), replace = False) for k in range(N)])
        Ts = np.mean(resamp[:,:len(surv_data)], axis=1) - np.mean(resamp[:,len(surv_data):], axis=1)
        Ts = [round(elem, 3) for elem in Ts]

        if st.percentileofscore(Ts, score=T) == 0:
            p_value = 0.5
        else:
            p_value = abs((st.percentileofscore(Ts, score=T) / 100) - 0.50)

    dataframe = pd.DataFrame()
    if len(sq_data) == 0:
        sq_mean = np.nan
    else:
        sq_mean = np.mean(sq_data)
    data_row = {'identity': identity, 'p_value': p_value, 'level': level, 'avg_' + g_var: benchmark, 'cov_perc': surv_count / divisor_size, 'tot_surv_props': surv_count, 'sq_mean': sq_mean}
    dataframe = dataframe.append(data_row, ignore_index=True)
    
    return dataframe

def get_monthized(surv_data_in, curryr, currmon, currqtr, l_var, g_var, sector_val):
    surv_data = surv_data_in.copy()

    surv_data = surv_data.rename(columns={'realid': 'id', 'ind_avrent': 'renx', 'ind_avail': 'availx'})

    # Create the sub and met identities
    surv_data = surv_data[surv_data['subid'].isnull() == False]
    surv_data['subid'] = surv_data['subid'].astype(int)
    surv_data['identity'] = surv_data['metcode'] + surv_data['subid'].astype(str) + surv_data['type2']
    surv_data['identity_met'] = surv_data['metcode'] + surv_data['type2']
    
    # Filter out surveyed data periods
    surv_data = surv_data[surv_data[l_var + 'M'] == 0]
    
    # Drop any surveyed observations that are more than 5 years from the current year
    surv_data = surv_data[surv_data['reisyr'] > curryr - 4]    
    
    # Generate a total survey count for each property, and drop cases where there is only 1 survey
    surv_data = surv_data.sort_values(by=['id', 'reisyr', 'reismon'], ascending=[True, False, False])
    surv_data['total_survcount'] = surv_data.groupby('id')['id'].transform('count')
    surv_data = surv_data[(surv_data['total_survcount'] > 1)]
    
    # Generate a survey counter from latest survey to earliest survey
    surv_data['surv_order'] = surv_data.groupby('id').cumcount()

    # Copy the dataframe so we can have one that will hold just the lagged surveys
    lagged = surv_data.copy()
    
    # If this is a quarter month and the sector is industrial and retail (who still use the old square code that only really moves non surveyed props in the q month), consider current surveys from the prior two months in addition to the ones in this month
    # Otherwise, a current survey is only one obtained in the current month
    print("Once ret and ind use redev square code, can take this out")
    if (currmon == 3 or currmon == 6 or currmon == 9 or currmon == 12) and (sector_val == "ind" or sector_val == "ret") and l_var == "renx":
        surv_data = surv_data[(surv_data['surv_order'] == 0)]
        surv_data['currtag'] = np.where((surv_data['reisyr'] == curryr) & (surv_data['reisqtr'] == currqtr), 1, 0)
        surv_data = surv_data[surv_data['currtag'] == 1]
        surv_data = surv_data.drop_duplicates('id')
        lagged = lagged[(lagged['surv_order'] > 0)]
        lagged['l_tag'] = np.where((lagged['reisyr'] != curryr) | ((lagged['reisyr'] == curryr) & (lagged['reisqtr'] < currqtr)), 1, 0)
        lagged = lagged[lagged['l_tag'] == 1]
    else:
        surv_data = surv_data[(surv_data['surv_order'] == 0)]
        surv_data['currtag'] = np.where((surv_data['reisyr'] == curryr) & (surv_data['reismon'] == currmon), 1,0)
        surv_data = surv_data[surv_data['currtag'] == 1]
        surv_data = surv_data.drop_duplicates('id')
        lagged = lagged[(lagged['surv_order'] > 0)]
        lagged['l_tag'] = np.where((lagged['reisyr'] != curryr) | ((lagged['reisyr'] == curryr) & (lagged['reismon'] < currmon)), 1, 0)
        lagged = lagged[lagged['l_tag'] == 1]
        
    # Sort the lagged dataset and drop any duplicates
    lagged.sort_values(by=['id', 'surv_order'],  ascending=[True, True], inplace = True)
    lagged = lagged.drop_duplicates('id')
    
    # Join in the lagged surveys to the current survey dataset, and calculate the monthized change.
    # At this point, the rent survey benchmark will use the 3 month monthized change, to give more oomph to the survey data (especially key in quarter months for sectors using the legacy square code)
    # For vacancy change, the survey benchmark will be the raw change, except in apartment, where it will be the three month monthized change
    if l_var == "renx":
        surv_data = surv_data.rename(columns={'survdate': 'mr_survdate',  'rnt_term': 'c_rnt_term', l_var: 'mr_rent'})
        lagged = lagged.rename(columns={'survdate': 'l_survdate', 'rnt_term': 'l_rnt_term', l_var: 'l_rent'})
        lagged = lagged[['id', 'l_survdate', 'l_rnt_term', 'l_rent']]
        surv_data = surv_data.join(lagged.set_index('id'), on='id')
        surv_data['mon_diff'] = ((surv_data['mr_survdate'] - surv_data['l_survdate'])/np.timedelta64(1, 'M'))
        surv_data['mon_diff'] = surv_data['mon_diff'].astype(int)
        surv_data['mon_diff'] = np.where(surv_data['mon_diff'] == 0, 1, surv_data['mon_diff'])
        if sector_val != "apt":
            surv_data['term_switch'] = np.where((surv_data['c_rnt_term'] != surv_data['l_rnt_term']) & (surv_data['l_rnt_term'].isnull() == False) & (surv_data['c_rnt_term'].isnull() == False), 1, 0)
        else:
            surv_data['term_switch'] = 0
        surv_data['surv_g'] = (surv_data['mr_rent'] - surv_data['l_rent']) / surv_data['l_rent']
        surv_data[g_var + '_1'] = surv_data['surv_g'] / surv_data['mon_diff']
        surv_data = surv_data[(surv_data['term_switch'] == 0) | (abs(surv_data[g_var + '_1']) <= 0.01)]
        surv_data[g_var] = surv_data[g_var + '_1'] * 3
        surv_data[g_var] = np.where((surv_data[g_var] > surv_data['surv_g']) & (surv_data['surv_g'] > 0), surv_data['surv_g'], surv_data[g_var])
        surv_data[g_var] = np.where((surv_data[g_var] < surv_data['surv_g']) & (surv_data['surv_g'] < 0), surv_data['surv_g'], surv_data[g_var])
        surv_data[g_var] = round(surv_data[g_var], 3)
        
        # Overwrite property growth rates that are in the extremes. Use a proxy that is in the same direction of the surveyed change, but less extreme
        # This will help avoid the impact of outliers
        surv_data['95_per'] = surv_data[g_var].quantile(q=0.95)
        surv_data['5_per'] = surv_data[g_var].quantile(q=0.05)
        surv_data['6_per'] = surv_data[g_var].quantile(q=0.06)
        surv_data['94_per'] =surv_data[g_var].quantile(q=0.94)
        surv_data[g_var] = np.where((surv_data[g_var] >= surv_data['95_per']), surv_data['94_per'], surv_data[g_var])
        surv_data[g_var] = np.where((surv_data[g_var] <= surv_data['5_per']), surv_data['6_per'], surv_data[g_var])
        #print("Take out export of surv data to csv!")
        #surv_data.to_csv("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}{}{}/OutputFiles/rent_surv_stats.csv".format(sector_val, curryr, "m", currmon))            
        surv_data = surv_data[['id', 'metcode', 'subid', 'type2', g_var, 'year', 'month', 'ind_size', 'identity', 'identity_met', 'mr_rent', 'l_rent']]
    elif l_var == "availx":
        surv_data = surv_data.rename(columns={'survdate': 'mr_survdate', l_var: 'mr_avail'})
        lagged = lagged.rename(columns={'survdate': 'l_survdate', l_var: 'l_avail'})
        surv_data = surv_data.set_index('id')
        lagged = lagged.set_index('id')
        lagged = lagged[['l_survdate', 'l_avail']]
        surv_data = surv_data.join(lagged)
        surv_data['mon_diff'] = ((surv_data['mr_survdate'] - surv_data['l_survdate'])/np.timedelta64(1, 'M'))
        surv_data['mon_diff'] = surv_data['mon_diff'].astype(int)
        surv_data['mon_diff'] = np.where(surv_data['mon_diff'] == 0, 1, surv_data['mon_diff'])
        if sector_val != "apt":
            surv_data[g_var] = (surv_data['mr_avail'] / surv_data['ind_size']) - (surv_data['l_avail'] / surv_data['ind_size'])
        elif sector_val == "apt":
            surv_data['surv_g'] = (surv_data['mr_avail'] / surv_data['ind_size']) - (surv_data['l_avail'] / surv_data['ind_size'])
            surv_data[g_var + '_1'] = (surv_data[g_var + '_1'] / surv_data['mon_diff'])
            surv_data[g_var] = surv_data['g_vacx_1'] * 3
            surv_data[g_var] = np.where((surv_data[g_var] > surv_data['surv_g']) & (surv_data['surv_g'] > 0), surv_data['surv_g'], surv_data[g_var])
            surv_data[g_var] = np.where((surv_data[g_var] < surv_data['surv_g']) & (surv_data['surv_g'] < 0), surv_data['surv_g'], surv_data[g_var])

            surv_data['95_per'] = surv_data['g_vacx'].quantile(q=0.95)
            surv_data['5_per'] = surv_data['g_vacx'].quantile(q=0.05)
            surv_data['6_per'] = surv_data['g_vacx'].quantile(q=0.06)
            surv_data['94_per'] =surv_data['g_vacx'].quantile(q=0.94)
            surv_data[g_var] = np.where((surv_data[g_var] >= surv_data['95_per']), surv_data['94_per'], surv_data[g_var])
            surv_data[g_var] = np.where((surv_data[g_var] <= surv_data['5_per']), surv_data['6_per'], surv_data[g_var])
        # print("Take out export of surv data to csv!")
        # surv_data.to_csv("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}{}{}/OutputFiles/avail_surv_stats.csv".format(sector_val, curryr, "m", currmon))
        surv_data = surv_data[['id', 'metcode', 'subid', 'type2', g_var, 'year', 'month', 'ind_size', 'identity', 'identity_met', 'mr_avail', 'l_avail']]
        
    return surv_data

# If this is a quarter month and a prop was surveyed in m1 or m2 or the quarter and is not changing in m3, tag it so that it will not be included in the square distribution (as the fact that it stayed flat this month is optimal, and does not depend on surveyed dist)
# Only for sectors still using legacy code
def get_prior_q_tag(surveys, all_data, filt_col, var, curryr, currmon):

    q_survs = surveys.copy()
    if currmon >= 3:
        q_survs = q_survs[(q_survs['currmon'] == currmon - 2) | (q_survs['currmon'] == currmon - 1)]
        q_survs = q_survs[(q_survs['yr'] == curryr)]
    elif currmon == 2:
        q_survs = q_survs[((q_survs['currmon'] == 1) & (q_survs['yr'] == curryr))  | ((q_survs['currmon'] == 12) & (q_survs['yr'] == curryr - 1))]
    elif currmon == 1:
        q_survs = q_survs[(q_survs['currmon'] == 12) | (q_survs['currmon'] == 11)]
        q_survs = q_survs[(q_survs['yr'] == curryr - 1)]
    q_survs = q_survs[q_survs[filt_col] == 0]
    q_survs = q_survs.drop_duplicates('id')
    q_survs['has_pq_surv_' + var] = 1
    q_survs = q_survs.set_index('id')
    q_survs = q_survs[['has_pq_surv_' + var]]
    surv_zero = all_data.copy()
    surv_zero['sq_no_g'] = np.where((surv_zero[filt_col] == 1) & (surv_zero[var] == surv_zero[var].shift(1)) & (surv_zero['id'] == surv_zero['id'].shift(1)) & (surv_zero['currmon'] == currmon) & (surv_zero['yr'] == curryr), 1, 0)
    surv_zero = surv_zero[surv_zero['sq_no_g'] == 1]
    surv_zero = surv_zero.set_index('id')
    surv_zero = surv_zero[['sq_no_g']]
    q_survs = q_survs.join(surv_zero)
    q_survs.dropna(axis=0, subset=['sq_no_g'], inplace =True)
    q_survs = q_survs[['has_pq_surv_' + var]]

    return q_survs

def g_dist_flags(sector_val, curryr, currmon, msq_data_in, program):

    if program == "ren_g":
        l_var = 'renx'
        g_var = 'g_renx'
        s_var = 'ind_avrent'
    elif program == 'vac_g':
        l_var = 'availx'
        g_var = 'g_vacx'
        s_var = 'ind_avail'

    currqtr = int(np.ceil(currmon / 3))

    msq_data = msq_data_in.copy()

    msq_data['sub_live'] = np.where(msq_data['submkt'].str[0:2] == "99", 0, 1)
    msq_data = msq_data[msq_data['sub_live'] == 1]

    msq_data.sort_values(by=['metcode', 'subid', 'id', 'yr', 'currmon'], inplace = True)

    msq_data = msq_data.rename(columns={'ind_size': 'sizex'})

    msq_data['type2'] = np.where(msq_data['type2'] == "F", "F", "DW")

    expansion_list = ["AA", "AB", "AK", "AN", "AQ", "BD", "BF", "BI", "BR", "BS", "CG", "CM", "CN", "CS", "DC", 
                    "DM", "DN", "EP", "FC", "FM", "FR", "GD", "GN", "GR", "HR", "HL", "HT", "KX", "LL", "LO", 
                    "LR", "LV", "LX", "MW", "NF", "NM", "NO", "NY", "OK", "OM", "PV", "RE", "RO", "SC", "SH", 
                    "SR", "SS", "ST", "SY", "TC", "TM", "TO", "TU", "VJ", "VN", "WC", "WK", "WL", "WS"]

    rent_comps_only_list = ['BD', 'CG', 'DC', 'HL', 'SS']

    if sector_val == "ind":
        msq_data = msq_data[(~msq_data['metcode'].isin(expansion_list)) | (msq_data['type2'] != "F")]
        msq_data = msq_data[(msq_data['subid'] != 77) | (msq_data['metcode'].isin(rent_comps_only_list))]
    
    msq_two = msq_data.copy()
    if currmon != 1:
        msq_two = msq_two[(msq_two['yr'] == curryr) & (msq_two['currmon'] >= currmon - 1)]
    else:
        msq_two = msq_two[((msq_two['yr'] == curryr) & (msq_two['currmon'] == currmon)) | (msq_two['yr'] == curryr - 1) & (msq_two['currmon'] == 12)]
    
    msq_surv = msq_data.copy()
    msq_surv = msq_surv[msq_surv['survdate'] != '']

    if l_var == "renx":
        msq_two[g_var] = np.where((msq_two['id'] == msq_two['id'].shift(1)), (msq_two[l_var] - msq_two[l_var].shift(1)) / msq_two[l_var].shift(1), np.nan)
        msq_two[g_var] = round(msq_two[g_var], 3)
    elif l_var == "availx":
        msq_two[g_var] = np.where((msq_two['id'] == msq_two['id'].shift(1)), (msq_two[l_var] / msq_two['sizex']) - (msq_two[l_var].shift(1) / msq_two['sizex'].shift(1)), np.nan)

    msq_curr = msq_two.copy()
    msq_curr = msq_curr[(msq_curr['yr'] == curryr) & (msq_curr['currmon'] == currmon)]

    surv_data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))
    
    surv_data['type2'] = np.where(surv_data['type2'] == "F", "F", "DW")
    
    surv_data[l_var + 'M'] = np.where(surv_data[s_var].isnull() == False, 0, 1)
    
    surv_data = get_monthized(surv_data, curryr, currmon, currqtr, l_var, g_var, sector_val)

    for dataframe in [msq_curr, msq_data]:
        dataframe.sort_values(by=['metcode', 'subid', 'type2', 'id'], inplace = True)
        dataframe['identity'] = dataframe['metcode'] + dataframe['subid'].astype(str) + dataframe['type2']
        dataframe['identity_met'] = dataframe['metcode'] + dataframe['type2']
    surv_data.sort_values(by=['metcode', 'subid', 'type2'], inplace = True)

    msq_curr['prop_count_sub'] = msq_curr.groupby('identity')['id'].transform('count')
    msq_curr['prop_count_met'] = msq_curr.groupby('identity_met')['id'].transform('count')

    cov_thresh_all_data = msq_curr.copy()
    cov_thresh_all_data = cov_thresh_all_data[(cov_thresh_all_data['yearx'] < curryr) | ((cov_thresh_all_data['yearx'] == curryr) & (cov_thresh_all_data['month'] < currmon) | (np.isnan(cov_thresh_all_data['yearx']) == True))]
    cov_thresh_all_data = cov_thresh_all_data[['identity', 'sizex']]
    cov_thresh_all_data = pd.DataFrame(cov_thresh_all_data.groupby('identity')['sizex'].count())
    cov_thresh_all_data.columns = ['tot_count_props']
    cov_thresh = surv_data.copy()
    cov_thresh = cov_thresh[(cov_thresh['year'] < curryr) | ((cov_thresh['year'] == curryr) & (cov_thresh['month'] < currmon) | (np.isnan(cov_thresh['year']) == True))]
    cov_thresh = pd.DataFrame(cov_thresh.groupby(['identity'])['ind_size'].count())
    cov_thresh.columns = ['surv_props']
    cov_thresh_all_data = cov_thresh_all_data.join(cov_thresh)
    cov_thresh_all_data['surv_props'] = cov_thresh_all_data['surv_props'].fillna(0)
    cov_thresh_all_data['surv_percent'] = cov_thresh_all_data['surv_props'] / cov_thresh_all_data['tot_count_props']
    # To determine the coverage threshold that determines what level of survey data the square data is evaluated on, use the 25th percentile of the fill rates if above 2%, otherwise max of half the surveyed fill mean and 2%
    # Cant use standard dev here, as the data is not normally distributed. And the percentile approach often yields a number that is too low, due to the many cases where a sub is not surveyed at all
    surv_mean = np.mean(cov_thresh_all_data['surv_percent'])
    if cov_thresh_all_data['surv_percent'].quantile(q=0.25) < 0.02:
        cov_thresh = max(surv_mean / 2, 0.02)
    else:
        cov_thresh = cov_thresh_all_data['surv_percent'].quantile(q=0.25)
    print(round(surv_mean,3), round(cov_thresh_all_data['surv_percent'].quantile(q=0.25),3), round(cov_thresh,3))

    print("Once ret and ind use redev square code, can take this out")
    if sector_val == "ind" or sector_val == "ret":
        q_survs = get_prior_q_tag(msq_surv, msq_data, l_var + 'M', l_var, curryr, currmon)
        msq_curr = msq_curr.join(q_survs, on='id')
    
    idents = list(msq_curr['identity'].unique())

    pool = mp.Pool(int(mp.cpu_count()*0.7))
    print("Once ret and ind use redev square code, take out drop of has_pq_surv")
    result_async = [pool.apply_async(permutation_test, args = (msq_curr, l_var, g_var, identity, curryr, currmon, sector_val, surv_data, cov_thresh, )) for identity in
                    idents]
    results = [r.get() for r in tqdm(result_async)]

    pool.close()

    all_p_vals = pd.DataFrame()
    all_p_vals = all_p_vals.append(results, ignore_index=True)
    all_p_vals.sort_values(by='p_value', ascending=False, inplace=True)
    all_p_vals = all_p_vals.reset_index(drop=True)

    benchmarks = all_p_vals.copy()
    benchmarks = benchmarks[['identity', 'avg_' + g_var]]

    all_subs = msq_curr.copy()
    all_subs = all_subs.drop_duplicates('identity')
    all_subs[['identity', 'metcode', 'subid', 'type2']]
    all_subs = all_subs.join(benchmarks.set_index('identity'), on='identity')
    benchmarks = all_subs.copy()
    benchmarks = benchmarks[['metcode', 'subid', 'type2', 'avg_' + g_var]]
    
    # Outsheet surv benchmarks for use in DQ's program
    benchmarks = benchmarks.rename(columns={'type2': 'indcat', 'avg_g_renx': 'avg_gren', 'avg_g_vacx': 'avg_gvac'})
    benchmarks['hedge'] = 0
    if l_var == 'renx':
        benchmarks.to_pickle("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/rent_surv_benchmarks_{}_{}m{}.pickle".format(sector_val, curryr, currmon, sector_val, curryr, currmon)) 
    elif l_var == "availx":
        benchmarks.to_pickle("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/avail_surv_benchmarks_{}_{}m{}.pickle".format(sector_val, curryr, currmon, sector_val, curryr, currmon))
    
    all_p_vals = all_p_vals.drop(['avg_' + g_var], axis=1)

    all_p_vals.to_pickle("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/{}_p_values.pickle".format(sector_val, curryr, currmon, l_var))
    surv_data.to_pickle("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/{}_surv_data.pickle".format(sector_val, curryr, currmon, l_var))
    
