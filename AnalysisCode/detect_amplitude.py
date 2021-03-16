# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 14:14:46 2020

@author: Tony_Tien
"""
import pandas as pd
import numpy as np
from tqdm import tqdm
import pickle

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

All_result = []
for key,value in tqdm(Have_cycle_source_data.groupby(['source_ip'])):
#    key = '192.168.88.178'
    value = Have_cycle_source_data.groupby(['source_ip']).get_group(key).copy()
    value['weekend'] = value['ds'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").weekday()+1)
    basis = value[(value['ds'] >= train_start_date) & (value['ds'] <= train_cut_date)]
    mode = 7
  
    All_mean = []
    for  i in range(int(len(value)/mode)):
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekend'])
        test_data = test_data_raw['unique_dest_cnt'].values
        All_mean.append(np.mean(test_data))
    All_log_mean = np.log(All_mean)
    
    Feature_df = pd.DataFrame({'mean':All_mean,'log_mean':All_log_mean})
    Feature_df.loc[0:int(len(basis)/mode)-1,'type'] = 'Training'
    Feature_df.loc[int(len(basis)/mode)::,'type'] = 'Testing'
        
        #跟basis的diff
    #    basis_high_iqr = basis['total_sent_bytes'].describe()['75%'] + ((basis['total_sent_bytes'].describe()['75%'] - basis['total_sent_bytes'].describe()['25%']))*1.5
    #    basis_low_iqr = basis['total_sent_bytes'].describe()['25%'] - ((basis['total_sent_bytes'].describe()['75%'] - basis['total_sent_bytes'].describe()['25%']))*1.5
    #    
    #    basis_filter = basis[(basis['total_sent_bytes'] > basis_low_iqr) & (basis['total_sent_bytes'] < basis_high_iqr)]
    #    basis_mean = np.mean(basis_filter['total_sent_bytes'])
    
    #    All_mean_diff = []
    #    for  i in range(int(len(value)/mode)):      
    #        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekend'])
    #        test_data = test_data_raw['total_sent_bytes'].values
    #        mean_diff = np.mean(test_data) - basis_mean
    #        All_mean_diff.append(mean_diff)
    #    Feature_df['mean_diff'] = All_mean_diff
    Training = Feature_df[Feature_df['type'] == 'Training']['mean'].values
    #plt.plot(Training , 'bo')
    #plt.ylabel('Mean of each period',fontsize = 12)
    #plt.xlabel('week',fontsize = 12)
    #plt.grid()   
    #plt.axhline(np.mean(basis_filter['total_sent_bytes']) + np.std(basis_filter['total_sent_bytes']), color='k',linestyle = '--')
    #plt.axhline(np.mean(basis_filter['total_sent_bytes']) - np.std(basis_filter['total_sent_bytes']), color='k',linestyle = '--')
    #plt.axhline(np.mean(basis_filter['total_sent_bytes']), color='m',linestyle = '--')
        
    clf = OneClassSVM(nu=0.2, kernel="rbf", gamma=0.1)
    normalize = preprocessing.scale(np.array(All_mean))
    train = normalize[0:int(len(basis)/mode)]
    test = normalize[int(len(basis)/mode):]
    
    train = np.reshape(train , (len(train),1))
    test = np.reshape(test , (len(test),1))
    Train_and_test = np.reshape(np.append(train[0:int(len(basis)/mode)] , test) , (len(np.append(train[0:int(len(basis)/mode)] , test)),1))
    clf.fit(train)
    clf.predict(test)
    predict = clf.predict(Train_and_test)
    Feature_df['if_abnormal'] = predict

    up_bound = np.mean(Training) + np.std(Training)
    low_bound = np.mean(Training) - np.std(Training)

    Feature_df.loc[(Feature_df['mean'] > low_bound) & (Feature_df['mean']  < up_bound),'if_abnormal_iqr'] = 1
    Feature_df['if_abnormal_iqr'] = Feature_df['if_abnormal_iqr'].fillna(-1)

    plt.figure()
    plt.scatter(np.arange(len(Feature_df)),Feature_df['mean'] , c = predict)
    plt.axvline(int(len(basis)/mode), color='k',linestyle = '--',label = 'train_cut')


    up_bound = max(Feature_df[(Feature_df['if_abnormal'] ==1) & (Feature_df['type'] =='Training')]['mean'])
    low_bound = min(Feature_df[(Feature_df['if_abnormal'] ==1) & (Feature_df['type'] =='Training')]['mean'])
#        up_bound = min(Feature_df[(Feature_df['if_abnormal'] ==-1) & (Feature_df['mean'] > normal_max)]['mean'])
#        low_bound = max(Feature_df[(Feature_df['if_abnormal'] ==-1) & (Feature_df['mean'] < normal_min)]['mean'])
#        up_bound = 1.8297095
#        low_bound = -1.5197436
    plt.xlabel('period')
    plt.ylabel('mean')
    plt.axhline(up_bound, color='m',linestyle = '--',label = 'up_bound')
    plt.axhline(low_bound, color='m',linestyle = '--',label = 'low_bound')
    plt.title('OCSVM (Mean)')
    plt.legend()
    
    Feature_df.loc[ (Feature_df.if_abnormal == -1) & (Feature_df.if_abnormal_iqr == -1),'Last_abnormal'] = -1
    Feature_df.loc[~( (Feature_df.if_abnormal == -1) & (Feature_df.if_abnormal_iqr == -1)),'Last_abnormal'] = 1
    predict = Feature_df['Last_abnormal'].values
    
    
    value['if_abnormal'] = np.nan 
    value['period'] = np.nan 
    source_data = value.copy().reset_index(drop=True)
    for  i in range(int(len(source_data)/mode)):
        source_data.loc[0+i*mode : (i+1)*mode,'if_abnormal'] = predict[i]
        source_data.loc[0+i*mode : (i+1)*mode,'period'] = int(i)
    
    
    
    #畫異常部分圖
    source_data = source_data[~source_data['if_abnormal'].isna()]
    normal_part = source_data[source_data['if_abnormal'] == 1]
    abnormal_part = source_data[source_data['if_abnormal'] == -1]
    normal_part1 = pd.merge(normal_part , source_data[['ds']],on = ['ds'],how='right')
    normal_part1 = normal_part1.sort_values(by = ['ds'])
    
    abnormal_part1 = pd.merge(abnormal_part , source_data[['ds']],on = ['ds'],how='right')
    abnormal_part1 = abnormal_part1.sort_values(by = ['ds'])
    
    
    plt.figure(figsize = (20,8))
    plt.plot(source_data['ds'],source_data['unique_dest_cnt'],'b')
    plt.plot(normal_part1['ds'],normal_part1['unique_dest_cnt'],'b',label = 'Mean Normal')
    plt.plot(abnormal_part1['ds'],abnormal_part1['unique_dest_cnt'],'lawngreen',label = 'Mean change')
    plt.title(key)
    plt.axvline(train_cut_date, color='k',linestyle = '--',label = 'train cut')
    plt.xlabel('ds')
    plt.ylabel('total_sent_bytes')
    plt.xticks(rotation = 90)
    plt.legend()
    plt.savefig('C:/Users/tony_tien/Desktop/git_project/peakdetection/PeriodChangeDetection/plot/{}.png'.format(key))
    plt.close()
    
 
