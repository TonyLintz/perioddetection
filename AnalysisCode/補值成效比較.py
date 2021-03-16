# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 10:47:08 2020

@author: Tony_Tien
"""


#================================================================================#

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import pickle
import concurrent.futures

from sklearn.svm import OneClassSVM
from sklearn import  ensemble, preprocessing, metrics
from datetime import datetime
from matplotlib import pyplot as plt
from DetrendMethod import detrend_with_lowpass
from config import *
from calculate import *
from ModelTraining import *
from plot_function import *

Have_cycle_source_data = pd.read_csv(data_path + '{}___Have_cycle_source_data.csv'.format(SourceDataType))
mode = 7
#SourceDataType = 'total_sent_bytes'

with open(pickle_path + f"{basis_set_name}.pkl", 'rb') as f:
    All_SIP_BASIS_DICT = pickle.load(f)

All_sip_corr = []
for key,value in Have_cycle_source_data.groupby(['source_ip']):
    
#   key = '172.21.132.5'
    value = Have_cycle_source_data.groupby(['source_ip']).get_group(key)
    
    basis = value.copy()
    weekend_median = basis.groupby(['weekdays'],as_index=False)[SourceDataType].median()
    baseline_list_pop = weekend_median[SourceDataType].values

    a_period_all_corr = []
    for i in range(int(len(value)/mode)):
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        test_data_raw_filter_na = test_data_raw[test_data_raw['is_server'] != '0'] 
        period_list_pop = test_data_raw[SourceDataType].values
        if (len(test_data_raw_filter_na) != mode) & (len(test_data_raw_filter_na) != 0):
              
               coeffi = stats.pearsonr(period_list_pop,baseline_list_pop)[0]
               if np.isnan(coeffi):
                    a_period_all_corr.append(1)
               else:  
                    a_period_all_corr.append(coeffi)
        else:
            pass
    All_sip_corr.extend(a_period_all_corr)
    
    



Have_cycle_source_data = pd.read_csv(data_path + '{}___Have_cycle_source_data(fillvalue).csv'.format(SourceDataType))
mode = 7
#SourceDataType = 'total_sent_bytes'

with open(pickle_path + f"{basis_set_name}.pkl", 'rb') as f:
    All_SIP_BASIS_DICT = pickle.load(f)

All_sip_new_corr = []
for key,value in Have_cycle_source_data.groupby(['source_ip']):
    
#   key = '172.17.32.34'
    value = Have_cycle_source_data.groupby(['source_ip']).get_group(key)

    basis = value.copy()
    weekend_median = basis.groupby(['weekdays'],as_index=False)[SourceDataType].median()
    baseline_list_pop = weekend_median[SourceDataType].values

    a_period_all_corr = []
    for i in range(int(len(value)/mode)):
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        test_data_raw_filter_na = test_data_raw[test_data_raw['is_server'] != '0'] 
        period_list_pop = test_data_raw[SourceDataType].values
        if (len(test_data_raw_filter_na) != mode) & (len(test_data_raw_filter_na) != 0):
              
               coeffi = stats.pearsonr(period_list_pop,baseline_list_pop)[0]
               if np.isnan(coeffi):
                    a_period_all_corr.append(1)
               else:  
                    a_period_all_corr.append(coeffi)
        else:
            pass
    All_sip_new_corr.extend(a_period_all_corr)
    
    
    
plt.boxplot([All_sip_corr , All_sip_new_corr],showfliers = True,labels = ['fill 0','fill Normal'])    
    
weekend_median = not_nan.groupby(['weekdays'],as_index=False)[SourceDataType].median().rename(columns = {SourceDataType:'median_of_cycle'})
    
sns.distplot(All_sip_corr, kde=False,label = 'fill 0 ')
sns.distplot(All_sip_new_corr, kde=False , label = 'fill normal')
plt.xlabel('correlation')
plt.ylabel('count')
plt.legend()