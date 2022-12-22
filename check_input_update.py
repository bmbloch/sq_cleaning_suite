import csv
import pandas as pd
import numpy as np
from pathlib import Path
from os import listdir
from os.path import isfile, join
import os
import re
import time
from IPython.core.display import display

pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)

# Read in the aggregated csv file that has all msqs in one file. This file is created by running the Load_Test_MSQS program
# Will check to ensure that the aggregated file modified date is fresher than that of the test msqs

def check_input_freshness(sector_val, curryr, currmon, program):

    file_status = "temp"

    pathlist_msqs = Path("/home/central/square/data/{}/production/msq/test".format(sector_val)).glob('**/*.dta')
    r_msqs = re.compile("..msq\.dta")

    root = Path("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs".format(sector_val))
    file_list = [f for f in listdir(root) if isfile(join(root, f))]
    if len(file_list) == 0:
        file_status = "msq"
    else:

        pathlist_aggreg = Path("/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/curr_msqs".format(sector_val)).glob('**/*.pickle')
        r_aggreg = re.compile("..msq\.dta")
        for path in pathlist_aggreg:
            path_in_str = str(path)
            aggreg_modified = os.path.getmtime(path_in_str)
            m_year_aggreg, m_month_aggreg, m_day_aggreg = time.localtime(aggreg_modified)[:-6]
            break

        isFile = os.path.isfile('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))
        if isFile == True: 
            logs_modified = os.path.getmtime('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/aggreg_logs.pickle'.format(sector_val, curryr, currmon))
            m_year_logs, m_month_logs, m_day_logs, m_hour_logs, m_min_logs = time.localtime(logs_modified)[:-4]
        else:
            file_status = "logs"

        if isFile == True and file_status != "logs" and program != "surv_bench":
            isFile = os.path.isfile('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/metro_avgs.pickle'.format(sector_val, curryr, currmon))
            if isFile == True:
                bench_modified = os.path.getmtime('/home/central/square/data/zzz-bb-test2/python/sq_redev/{}/{}m{}/OutputFiles/metro_avgs.pickle'.format(sector_val, curryr, currmon))
                m_year_bench, m_month_bench, m_day_bench, m_hour_bench, m_min_bench = time.localtime(bench_modified)[:-4]
                if (m_year_logs > m_year_bench) or (m_year_logs == m_year_bench and m_month_logs > m_month_bench) or (m_year_logs == m_year_bench and m_month_logs == m_month_bench and m_day_logs > m_day_bench) or (m_year_logs == m_year_bench and m_month_logs == m_month_bench and m_day_logs == m_day_bench and m_hour_logs > m_hour_bench) or (m_year_logs == m_year_bench and m_month_logs == m_month_bench and m_day_logs == m_day_bench and m_hour_logs == m_hour_bench and m_min_logs > m_min_bench):
                    file_status = "bench"
            else:
                file_status = "bench"

        if isFile == True and file_status != "bench":
    
            file_status = "refreshed"
            for path in pathlist_msqs:
                path_in_str = str(path)
                testing = path_in_str[-9:]
                if r_msqs.match(testing) != None:
                    modified = os.path.getmtime(path)
                    m_year_msq, m_month_msq, m_day_msq = time.localtime(modified)[:-6]
                    if m_year_msq < curryr or (m_year_msq == curryr and m_month_msq < currmon):
                        file_status = "oob"
                    else:
                        if (m_year_msq < m_year_aggreg) or (m_year_msq == m_year_aggreg and m_month_msq < m_month_aggreg) or (m_year_msq == m_year_aggreg and m_month_msq == m_month_aggreg and m_day_msq <= m_day_aggreg):
                            if (m_year_msq < m_year_logs) or (m_year_msq == m_year_logs and m_month_msq < m_month_logs) or (m_year_msq == m_year_logs and m_month_msq == m_month_logs and m_day_msq <= m_day_logs):
                                file_status = "refreshed"
                            else:
                                file_status = "logs"
                        else:
                            file_status = "msq"
                        break
    
    return file_status
