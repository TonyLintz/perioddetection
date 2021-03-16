# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 22:31:03 2020

@author: Tony_Tien
"""
import pandas as pd
import numpy as np
from tqdm import tqdm
import pickle
import concurrent.futures
import time
from tqdm import tqdm
import shutil

from sklearn.svm import OneClassSVM
from sklearn import  ensemble, preprocessing, metrics
from datetime import datetime
from matplotlib import pyplot as plt
from DetrendMethod import detrend_with_lowpass
from config import *
from log import Log
from calculate import *
from ModelTraining import *
from plot_function import *
from Calculate_PR_Indicator import *

log = Log(__name__).getlog()

def plot_job(key, model, basis, predict, predict_ocsvm, Feature_df, Feature_df_ocsvm, source_data, weekend_median):
    #畫特徵分類圖        
    plot_feature_scatter(basis, predict_ocsvm, Feature_df_ocsvm, key, False)
   
    #畫異常部分圖
    plot_normal_abnormal_in_data(source_data,key,False)
    
    #畫每一期圖
    plot_normal_abnormal_period(Feature_df,source_data,key,flag=False)
    
    #畫該source 普遍pattern
    plot_cycle_general(SourceDataType,weekend_median,key,flag=False)


def main_job(key,Have_cycle_source_data, All_Model_InfoDict, All_SIP_FEATURE_DICT, All_SIP_BASIS_DICT):
        #key = '10.3.0.73'
        value = Have_cycle_source_data.groupby(['source_ip']).get_group(key).copy()    
 
        model = All_Model_InfoDict[key]
        basis = All_SIP_BASIS_DICT[key]
        Feature_df = All_SIP_FEATURE_DICT[key]
                
        Predict = Modeling_predict(Feature_df.loc[:,'FFT_Amp'].values,model)
        Feature_df['if_abnormal'] = Predict
        Feature_df = transform_label(Feature_df)
        Feature_df_ocsvm = Feature_df.copy()
        predict_ocsvm = Feature_df_ocsvm['if_abnormal'].values
        Feature_df = Resurrection(Feature_df)
        predict = Feature_df['if_abnormal'].values
       
        value['if_abnormal'] = np.nan 
        value['period'] = np.nan 
        source_data = value.copy().reset_index(drop=True)
        for  i in range(int(len(source_data)/mode)):
            source_data.loc[0+i*mode : (i+1)*mode-1,'if_abnormal'] = predict[i]
            source_data.loc[0+i*mode : (i+1)*mode-1,'period'] = int(i)
        
        weekend_median = basis.groupby(['weekdays'],as_index=False)[SourceDataType].median()
        weekend_median = weekend_median.rename(columns = {SourceDataType:'median_of_cycle'})
        
        if SourceDataType == 'total_sent_bytes':
            weekend_median['median_of_cycle'] = weekend_median['median_of_cycle'] / (1024*1024)
        else:
            pass
       
        source_data = pd.merge(source_data , weekend_median , on =['weekdays'])
        
        #計算for PR、Rank Percentile 的 indicator
        source_data = Period_calcu_PR_indicator(source_data)
        source_data = source_data.sort_values(by = ['ds'])
        #畫圖部分
        plot_job(key, model, basis, predict, predict_ocsvm, Feature_df, Feature_df_ocsvm, source_data, weekend_median)
        
        return source_data
    

if __name__ == "__main__":
    print(parser)
    print (("\n"))
    print (("SourceDataType is : " + SourceDataType)) 
    print (("if_to_current is : " + str(if_to_current))) 
    print (("if_move_file is : " + str(if_move_file))) 
    
    log.info("Start: load Model and Feature and basis pickle")
    with open(pickle_path + f"{model_name}.pkl", 'rb') as f:
        All_Model_InfoDict = pickle.load(f)
    
    with open(pickle_path + f"{feature_set_name}.pkl", 'rb') as f:
        All_SIP_FEATURE_DICT = pickle.load(f)
    
    with open(pickle_path + f"{basis_set_name}.pkl", 'rb') as f:
        All_SIP_BASIS_DICT = pickle.load(f)
    log.info("End: load Model and Feature and basis pickle")
        
    log.info("Start: Read have cycle of server csv")
    Have_cycle_source_data = pd.read_csv(data_path + '{}___Have_cycle_source_data.csv'.format(SourceDataType))
    log.info("End: Read have cycle of server csv")
    
    log.info("Start: Detect anomaly period")
    start_time_2 = time.time()
    All_result = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=core_max) as executor:
        futures = [executor.submit(main_job, key, Have_cycle_source_data, All_Model_InfoDict, All_SIP_FEATURE_DICT, All_SIP_BASIS_DICT) for key in Have_cycle_source_data.groupby(['source_ip']).size().index.tolist()]
        for Result in tqdm(concurrent.futures.as_completed(futures)):
                All_result.append(Result.result())
   
    log.info("End: Detect anomaly period")
    #存取最終結果Table
    All_result_df = pd.concat(All_result)
    All_result_df.to_csv(data_path + 'period_{}_result_file.csv'.format(SourceDataType_Small_Name),index=False)
    print ("Process pool execution in " + str(time.time() - start_time_2), "seconds")
    
    log.info("Start: Move Result file to Production Server")
    if if_move_file:
        shutil.copy2(data_path + 'period_{}_result_file.csv'.format(SourceDataType_Small_Name),  share_result_path + 'period_{}_result_file.csv'.format(SourceDataType_Small_Name))
    else:
        pass
    log.info("End: Move Result file to Production Server")