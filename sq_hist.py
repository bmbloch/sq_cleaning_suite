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

def sq_hist_flags(sector_val, curryr, currmon, msq_data_in, p_msq_data_in):

    new_survs = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/new_survs.pickle'.format(sector_val, curryr, currmon))

    new_survs = new_survs[['realid', 'reisyr', 'reismon', 'new_rent', 'diff_rent', 'removed_rent', 'new_opex', 'diff_opex', 'removed_opex', 'new_tax', 'diff_tax', 'removed_tax', 'new_avail', 'diff_avail', 'removed_avail']]
    new_survs = new_survs.drop_duplicates(['realid', 'reisyr', 'reismon'], axis=1)
    new_survs['identity_period'] = new_survs['realid'].astype(str) + new_survs['reisyr'].astype(str) + new_survs['reismon'].astype(str)
    new_survs = new_survs.drop(['realid', 'reisyr', 'reismon'], axis=1)

    msq_data = msq_data_in.copy()
    msq_data['currtag'] = np.where((msq_data['yr'] == curryr) & (msq_data['currmon'] == currmon), 1, 0)
    msq_data = msq_data[msq_data['currtag'] == 0]
    msq_data = msq_data[['id', 'realid', 'yr', 'currmon', 'qtr', 'renx', 'opex', 'taxx', 'availx', 'renxM', 'availxM']]
    msq_data['identity_period'] = msq_data['realid'].astype(str) + msq_data['yr'].astype(str) + msq_data['currmon'].astype(str)

    p_msq_data = p_msq_data_in.copy()
    p_msq_data = p_msq_data[['id', 'renx', 'opex', 'taxx', 'availx']]
    p_msq_data = p_msq_data.rename(columns={'renx': 'p_renx', 'opex': 'p_opex', 'taxx': 'p_taxx', 'availx': 'p_availx'})
    
    msq_data = msq_data.join(p_msq_data.set_index('id'), on='id')
    msq_data['renx_diff'] = np.where((msq_data['renx'] != msq_data['p_renx']) & (msq-data['p_renx'].isnull() == False), 1, 0)
    msq_data['opex_diff'] = np.where((msq_data['opex'] != msq_data['p_opex']) & (msq-data['p_opex'].isnull() == False), 1, 0)
    msq_data['taxx_diff'] = np.where((msq_data['taxx'] != msq_data['p_taxx']) & (msq-data['p_taxx'].isnull() == False), 1, 0)
    msq_data['availx_diff'] = np.where((msq_data['availx'] != msq_data['p_availx']) & (msq-data['p_availx'].isnull() == False), 1, 0)

    msq_data['sum_diffs'] = msq_data[['renx_diff', 'opex_diff', 'taxx_diff', 'availx_diff']].sum(axis=1)
    msq_data = msq_data[msq_data['sum_diffs'] > 0]
    
    msq_data = msq_data.join(new_survs.set_index('identity_period'), on='identity_period')

    msq_data['rent_flag'] = np.where((msq_data['new_rent'] == 1) & (msq_data['diff_rent'] == 1) & (msq_data['removed_rent'] == 1), 1, 0)
    msq_data['opex_flag'] = np.where((msq_data['new_opex'] == 1) & (msq_data['diff_opex'] == 1) & (msq_data['removed_opex'] == 1), 1, 0)
    msq_data['tax_flag'] = np.where((msq_data['new_tax'] == 1) & (msq_data['diff_tax'] == 1) & (msq_data['removed_tax'] == 1), 1, 0)
    msq_data['avail_flag'] = np.where((msq_data['new_avail'] == 1) & (msq_data['diff_avail'] == 1) & (msq_data['removed_avail'] == 1), 1, 0)

    flag_cols = ['rent_flag', 'opex_flag', 'tax_flag', 'avail_flag']

    msq_data['sum_flags'] = msq_data[flag_cols].sum(axis=1)

    msq_data_filt = msq_data.copy()
    msq_data_filt = msq_data_filt[msq_data_filt['sum_flags'] > 1]
    msq_data_filt['flag_period'] = msq_data_filt['currmon'].astype(int).astype(str) + "/" + msq_data_filt['yr'].astype(int).astype(str)
    msq_data_filt = msq_data_filt[['id', 'identity', 'metcode', 'subid', 'yr', 'currmon', 'renx', 'p_renx', 'opex', 'p_opex', 'taxx', 'p_taxx', 'availx', 'p_availx', 'rent_flag', 'opex_flag', 'tax_flag', 'avail_flag']]
    msq_data_filt.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sq_hist_flags.pickle'.format(sector_val, curryr, currmon))