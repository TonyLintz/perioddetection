# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 18:29:26 2020

@author: Tony_Tien
"""
import sys
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import pickle
import concurrent.futures
from datetime import datetime

from config import *
from log import Log
from process import fill_value_job
from calculate import calculate_overall_feature
from ModelTraining import Period_modeltraining

log = Log(__name__).getlog()

def main_job(key, Have_cycle_source_data, if_training):
    value = Have_cycle_source_data.groupby(['source_ip']).get_group(key).copy()
    value['weekdays'] = value['ds'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").weekday()+1)
    basis = value[(value['ds'] >= train_start_date) & (value['ds'] <= train_cut_date)]
    
   #去除有非六日holiday的期數        
    New_value = pd.DataFrame()
    have_holiday_index = []
    for u in range(int(len(value)/mode)):  
        test_data_raw = value[0+u*mode : (u+1)*mode].sort_values(by = ['weekdays'])
        if 'Non-weekend-holiday' in test_data_raw['holiday_type'].values:
            pass
        else:
            New_value = pd.concat([New_value , test_data_raw])
            have_holiday_index.append(u)
    New_value = New_value.sort_values(by = ['ds'])
    basis_filter_hday = New_value[(New_value['ds'] >= train_start_date) & (New_value['ds'] <= train_cut_date)]
    
    All_amp = calculate_overall_feature(value,basis_filter_hday,mode)
    Feature_df = pd.DataFrame({'FFT_Amp': All_amp})
    Feature_df.loc[0:int(len(basis)/mode)-1,'type'] = 'Training'
    Feature_df.loc[int(len(basis)/mode)::,'type'] = 'Testing'
 
    if if_training:
        Feature_filter_hday = Feature_df.loc[(have_holiday_index)]
        model_info = Period_modeltraining(Feature_filter_hday.loc[Feature_filter_hday.type == 'Training','FFT_Amp'].values,key) 
        return model_info,Feature_df,basis_filter_hday,key
    else:
        return Feature_df,key
        



if __name__ == "__main__":
    print(parser)
    print (("\n"))
    print (("SourceDataType is : " + SourceDataType)) 
    print (("if_to_current is : " + str(if_to_current))) 
    print (("if_training is : " + str(if_training))) 
    
    start_time_2 = time.time()
    log.info("Start: Read have cycle of server csv")
    Have_cycle_source_data = pd.read_csv(data_path + '{}___Have_cycle_source_data.csv'.format(SourceDataType))
    log.info("End: Read have cycle of server csv")

    #=============補值演算法==========#
    log.info("Start: fill empty value and rescaling the data")
    All_SIP_Fill_Data = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=core_max) as executor:
        futures = [executor.submit(fill_value_job, value, mode, SourceDataType) for key,value in Have_cycle_source_data.groupby(['source_ip'])]
        for Result in tqdm(concurrent.futures.as_completed(futures)):
                All_SIP_Fill_Data.append(Result.result())
    Have_cycle_source_data = pd.concat(All_SIP_Fill_Data)    
    Have_cycle_source_data.to_csv(data_path + '{}___Have_cycle_source_data.csv'.format(SourceDataType),index=False)
   
    if SourceDataType == 'total_sent_bytes':
        Have_cycle_source_data[SourceDataType] = pow(Have_cycle_source_data[SourceDataType], 1/3)
    log.info("End: fill empty value and rescaling the data")    

    #=============特徵製作 & 模型訓練==========#    
    
    if if_training:
       
        log.info("Start: Training Model and feature extraction")
        All_Model_InfoDict = {}
        All_SIP_FEATURE_DICT = {}
        All_SIP_BASIS_DICT = {}
        with concurrent.futures.ProcessPoolExecutor(max_workers=core_max) as executor:
            futures = [executor.submit(main_job, key, Have_cycle_source_data, if_training) for key in Have_cycle_source_data.groupby(['source_ip']).size().index.tolist()]
            for ResultGroup in tqdm(concurrent.futures.as_completed(futures)):
                    All_Model_InfoDict.update(ResultGroup.result()[0])
                    All_SIP_FEATURE_DICT.update({f"{ResultGroup.result()[3]}": ResultGroup.result()[1]})
                    All_SIP_BASIS_DICT.update({f"{ResultGroup.result()[3]}": ResultGroup.result()[2]})
        log.info("End: Training Model and feature extraction")
        
        #存取各Source IP model 資訊與特徵集
        with open(pickle_path + f"{model_name}.pkl", "wb") as file:
            pickle.dump(All_Model_InfoDict, file)
            
        with open(pickle_path + f"{feature_set_name}.pkl", "wb") as file:
            pickle.dump(All_SIP_FEATURE_DICT, file)
        
        with open(pickle_path + f"{basis_set_name}.pkl", "wb") as file:
            pickle.dump(All_SIP_BASIS_DICT, file)
    
    else:
        
        log.info("Start: Feature extraction")
        All_SIP_FEATURE_DICT = {}
        with concurrent.futures.ProcessPoolExecutor(max_workers=core_max) as executor:
            futures = [executor.submit(main_job, key, Have_cycle_source_data, if_training) for key in Have_cycle_source_data.groupby(['source_ip']).size().index.tolist()]
            for ResultGroup in tqdm(concurrent.futures.as_completed(futures)):
                     All_SIP_FEATURE_DICT.update({f"{ResultGroup.result()[1]}": ResultGroup.result()[0]})
      
        with open(pickle_path + f"{feature_set_name}.pkl", "wb") as file:
            pickle.dump(All_SIP_FEATURE_DICT, file)
        log.info("End: Feature extraction")
    print ("Process pool execution in " + str(time.time() - start_time_2), "seconds")
    