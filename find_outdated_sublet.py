# Use this program to identify cases where the an outdate sublet survey needs to be nulled out

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
from IPython.core.display import display, HTML

def sublet_flags(sector_val, curryr, currmon):
    # Load in the aggregated log file data. This data was saved by the Load_Logs program.
    data = pd.read_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))

    mr_sublet = data.copy()
    mr_sublet = mr_sublet[mr_sublet['sublet'].isnull() == False]
    mr_sublet.sort_values(by=['realid', 'survyr', 'survmon', 'survday'], ascending=[True, False, False, False], inplace=True)
    mr_sublet = mr_sublet.drop_duplicates('realid')
    mr_sublet = mr_sublet[['sublet', 'realid', 'survdate']]
    mr_sublet = mr_sublet.rename(columns={'sublet': 'mr_sublet', 'survdate': 'mr_survdate'})
    mr_sublet = mr_sublet.set_index('realid')
    data = data.join(mr_sublet, on='realid')

    data['sublet_flag'] = np.where((data['ind_avail'] + data['mr_sublet'] > data['ind_size']) & (data['ind_avail'] <= data['ind_size']) & (data['mr_sublet'].isnull() == False) & (data['survdate'] >= data['mr_survdate']), 1, 0)

    data = data[data['sublet_flag'] == 1]
    data = data[data['survdate'] >= data['mr_survdate']]
    data.sort_values(by=['realid', 'survyr', 'survmon', 'survday'], ascending=[True, True, True, True], inplace=True)
    data = data.drop_duplicates('realid')
    data['type2'] = np.where(data['type2'] == "F", "F", "DW")
    data = data[data['subid'].isnull() == False]
    data['identity'] = data['metcode'] + data['subid'].astype(int).astype(str) + data['type2']

    data['flag_period'] = data['reismon'].astype(int).astype(str) + "/" + data['reisyr'].astype(int).astype(str)

    data = data[['identity', 'realid', 'metcode', 'subid', 'ind_size', 'survdate', 'ind_avail', 'mr_sublet', 'mr_survdate', 'sublet_flag', 'flag_period']]

    data = data.rename(columns={'realid': 'id'})
    
    data.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/sublet_check_flags.pickle'.format(sector_val, curryr, currmon))

    flag_cols = ['sublet_flag']

    return flag_cols