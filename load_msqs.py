
# Use this program to aggregate the individual metro test MSQs into one large dataframe, to be used by subsequent programs
import csv
import pandas as pd
import numpy as np
from pathlib import Path
import multiprocessing as mp
from tqdm import tqdm
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)

def load_msqs(sector_val, curryr, currmon, msq_trunc_cols, file_path):

    msq = pd.read_stata(file_path)
    metcode = file_path[-9:-7]
    
    msq.sort_values(by=['metcode', 'id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True], inplace=True)
    msq = msq.reset_index(drop=True)
    msq['type2_temp'] = np.where(msq['type2'] == "F", "F", "DW")
    msq['identity'] = msq['metcode'] + msq['subid'].astype(str) + msq['type2_temp']
    msq = msq.drop(['type2_temp'], axis=1)
    if "final" in file_path:
        msq = msq[msq['id'].isnull() == False]
    msq['id'] = msq['id'].astype(int)

    msq['Gmrent'] = np.where((msq['id'] == msq['id'].shift(1)), (msq['renx'] - msq['renx'].shift(1)) / msq['renx'].shift(1), np.nan)
    msq['vac_chg'] = np.where((msq['id'] == msq['id'].shift(1)), msq['totvacx'] - msq['totvacx'], np.nan)
    
    if "final" in file_path:
        out_path = '/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/prior_msqs/{}msq.pickle'.format(sector_val, metcode)
    else:
        out_path = '/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs/{}msq.pickle'.format(sector_val, metcode)

    msq.to_pickle(out_path)

    if "final" not in file_path:
        sub_name = msq.copy()
        sub_name = sub_name[['identity', 'submkt']]
        sub_name = sub_name.drop_duplicates('identity')

        out_path_trunc = '/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/trunc_msqs/{}msq_trunc.pickle'.format(sector_val, metcode)
        msq_trunc = msq.copy()
        
        expansion_list = ["AA", "AB", "AK", "AN", "AQ", "BD", "BF", "BI", "BR", "BS", "CG", "CM", "CN", "CS", "DC", 
                    "DM", "DN", "EP", "FC", "FM", "FR", "GD", "GN", "GR", "HR", "HL", "HT", "KX", "LL", "LO", 
                    "LR", "LV", "LX", "MW", "NF", "NM", "NO", "NY", "OK", "OM", "PV", "RE", "RO", "SC", "SH", 
                    "SR", "SS", "ST", "SY", "TC", "TM", "TO", "TU", "VJ", "VN", "WC", "WK", "WL", "WS"] 

        rent_comps_only_list = ['BD', 'CG', 'DC', 'HL', 'SS']
        
        msq_trunc = msq_trunc[(msq_trunc['metcode'].isin(expansion_list) == False) | (msq_trunc['type2'] != "F")]
        msq_trunc = msq_trunc[(msq_trunc['subid'] != 77) | (msq_trunc['metcode'].isin(rent_comps_only_list))]
        msq_trunc = msq_trunc[msq_trunc['submkt'].str.contains('99') == False]
        msq_trunc = msq_trunc[msq_trunc['submkt'] != '']    
        
        if currmon == 1:
            msq_trunc = msq_trunc[((msq_trunc['yr'] == curryr) & (msq_trunc['currmon'] == currmon)) | ((msq_trunc['yr'] == curryr - 1) & (msq_trunc['currmon'] == 12))]
        else:
            msq_trunc = msq_trunc[((msq_trunc['yr'] == curryr) & (msq_trunc['currmon'] == currmon)) | ((msq_trunc['yr'] == curryr) & (msq_trunc['currmon'] == currmon - 1))]

        msq_trunc = msq_trunc[msq_trunc_cols]
        msq_trunc.to_pickle(out_path_trunc)
    else:
        sub_name = []

    return sub_name


def set_pool(directory_in_str, sector_val, curryr, currmon, msq_trunc_cols):

    pathlist = Path(directory_in_str).glob('**/*.dta')
    paths = []
    for path in pathlist:
        path_in_str = str(path)
        if "premsq" in path_in_str or "prep" in path_in_str or "ids_" in path_in_str: 
            False
        else:
            paths.append(path_in_str)

    pool = mp.Pool(int(mp.cpu_count()*0.7))
    result_async = [pool.apply_async(load_msqs, args = (sector_val, curryr, currmon, msq_trunc_cols, path, )) for path in
                    paths]
    results = [r.get() for r in tqdm(result_async)]

    if "final" not in directory_in_str:
        sub_names = pd.DataFrame()
        sub_names = sub_names.append(results, ignore_index=True)
        sub_names.sort_values(by=['identity'], ascending=[True], inplace=True)
        sub_names = sub_names.reset_index(drop=True)
        sub_names.to_csv('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}_sub_names.csv'.format(sector_val, sector_val), index=False)

    pool.close()

def convert_msqs(sector_val, curryr, currmon):

    if currmon == 1:
        pastyr = curryr - 1
        pastmon = 12
    else:
        pastyr = curryr
        pastmon = currmon - 1

    msq_trunc_cols = ['id', 'realid', 'yr', 'qtr', 'currmon', 'type2', 'metcode', 'subid', 'submkt', 'survdate', 'yearx', 'month', 'avrent', 're_tax', 'op_exp', 'rnt_term', 'renx', 'renxM',  'taxx', 'taxxM', 'opex', 'opexM', 'availx', 'availxM', 'totavailx', 'sublet', 'subletxM', 'ind_size', 'vacratx', 'totvacx', 'termx', 'termxM']

    print("Update directory to live folder when new sq code goes live")
    directory_in_str_curr = '/home/central/square/data/{}/production/msq/test'.format(sector_val)
    directory_in_str_past = '/home/central/square/data/{}/production/{}m{}_final_squaredmsqs'.format(sector_val, pastyr, pastmon)
    try:
        set_pool(directory_in_str_curr, sector_val, curryr, currmon, msq_trunc_cols)
        try:
            set_pool(directory_in_str_past, sector_val, curryr, currmon, msq_trunc_cols)
            message = "MSQs Processed Succesfully. Click the x to return to the program selection screen"
            color = 'success'
        except:
            message = "There was an issue processing the past period MSQs. Click the x to return to the program selection screen"
            color = 'danger'
    except:
        message = "There was an issue processing the current period MSQs. Click the x to return to the program selection screen"
        color = 'danger'

    return message, color
    

