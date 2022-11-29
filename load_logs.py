# Use this program to load the archived log files that contain the Foundation survey data for the period selected

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
from IPython.core.display import display, HTML

def load_log_files(sector_val, curryr, currmon):

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

    # Set the path to the input file
    directory_in_str = '/home/central/square/data/{}/download/{}m{}'.format(sector_val, curryr, currmon)
    pathlist = Path(directory_in_str).glob('**/*.log')

    # Get the data from the nightly download log files, and load it all into a dataframe
    count_files = 0
    count_met = 0
    df_cols = []
    data_list = []
    error_list = []
    id_error_list = []
    for path in pathlist:
        count_cols = 0
        path_in_str = str(path)
        with open(path_in_str, "r") as file:
            metcode = path_in_str[-6:-4]
        if metcode in pub_list:
            count_files += 1
            with open(path_in_str, "r") as file:
                for line in file:
                    line_split = line.split(",")
                    if count_cols == 0:
                        df_cols = line_split
                        df_cols = [x.lower() for x in df_cols]
                        if count_met == 0:
                            data = pd.DataFrame(columns = df_cols)
                            count_met = 1
                        count_cols = 1
                    else:
                        if len(line_split) == len(df_cols):
                            data_list.append(line_split)
                        else:
                            error_list.append(line_split)
                            if line_split[0].replace('"', '') not in id_error_list:
                                id_error_list.append(line_split[0].replace('"', ''))
        if count_files == len(pub_list):
            break

    data = data.append(pd.DataFrame(data_list, columns = df_cols))
    #np.savetxt("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/error_list.csv".format(sector_val, curryr, currmon),  id_error_list, delimiter =", ",  fmt ='% s') 

    # Strip out the quotations from each data point
    for i, col in enumerate(data.columns):
        data.iloc[:, i] = data.iloc[:, i].str.replace('"', '')
        
    data[['survmon','survday', 'survyr']] = data['survdate'].str.split("/",expand=True,)
    data[['survmon', 'survday', 'survyr']] = data[['survmon', 'survday', 'survyr']].astype(int)
    data['reismon'] = np.where((data['survday'] <= 15), data['survmon'], data['survmon'] + 1)
    data['reismon'] = np.where((data['survday'] > 15) & (data['survmon'] == 12), 1, data['reismon'])
    data['reisyr'] = np.where((data['survmon'] != 12), data['survyr'], data['survyr'] + 1)
    data['reisyr'] = np.where((data['survmon'] == 12) & (data['survday'] <= 15), data['survyr'], data['reisyr'])

    data['survdate']= pd.to_datetime(data['survdate'])

    for col in list(data.columns):
        if col != "survdate":
            data[col] = np.where(data[col] == '', np.nan, data[col])

    data['sublet'] = data['sublet'].astype(float)
    data['ind_avail'] = data['ind_avail'].astype(float)
    data['ind_size'] = data['ind_size'].astype(float)
    data['ind_avrent'] = data['ind_avrent'].astype(float)
    data['op_exp'] = data['op_exp'].astype(float)
    data['re_tax'] = data['re_tax'].astype(float)
    data['realid'] = data['realid'].astype(int)
    data['year'] = data['year'].astype(float)
    data['month'] = data['month'].astype(float)
    data['subid'] = data['subid'].astype(float)

    sub_names = pd.read_csv('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}_sub_names.csv'.format(sector_val, sector_val))
    data['type2_temp'] = np.where(data['type2'] == "F", "F", "DW")
    data['subid_temp'] = data['subid']
    data['subid_temp'] = data['subid_temp'].fillna(9999999)
    data['identity'] = data['metcode'] + data['subid_temp'].astype(int).astype(str) + data['type2_temp']
    data = data.join(sub_names.set_index('identity'), on='identity')
    data = data.drop(['identity', 'type2_temp', 'subid_temp'], axis=1)

    try:
        data.to_pickle('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))
        message = "Logs Aggregated Succesfully. Click the x to return to the program selection screen"
        color = 'success'
    except:
        message = "There was an issue loading the logs. Aggregation Not Successful. Click the x to return to the program selection screen"
        color = 'danger'

    return message, color, id_error_list