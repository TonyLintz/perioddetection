# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 10:34:48 2020

@author: Tony_Tien
"""

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
list_ = ['172.16.98.87',
'172.22.128.36',
'172.22.196.139',
'172.21.128.28',
'172.21.130.8',
'172.22.196.134',
'172.21.132.151',
'192.168.81.141',
'172.21.129.32',
'172.16.98.224',
]
Have_cycle_source_data = Have_cycle_source_data[Have_cycle_source_data['source_ip'].isin(list_)]
All_result = []
for key,value in tqdm(Have_cycle_source_data.groupby(['source_ip'])):
#    key = '192.168.88.178'
    value = Have_cycle_source_data.groupby(['source_ip']).get_group(key).copy()
#    value['trend_total_sent_bytes'] = detrend_with_wavelet(value['total_sent_bytes'])[2][0:len(value)]
    value['weekdays'] = value['ds'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").weekday()+1)
    basis = value[(value['ds'] >= train_start_date) & (value['ds'] <= train_cut_date)]
    mode = 7

  
    All_std = []
    for  i in range(int(len(value)/mode)): 
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        test_data = test_data_raw[SourceDataType].values
        All_std.append(process_std(test_data))

    All_mean = []
    for  i in range(int(len(value)/mode)):
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        test_data = test_data_raw[SourceDataType].values
        All_mean.append(np.mean(test_data))
    All_log_mean = np.log(All_mean)

    
#    All_log_mean = np.log(All_mean)
    
    Feature_df = pd.DataFrame({'mean':All_mean,'log_mean':All_log_mean,'std':All_std})
    Feature_df.loc[0:int(len(basis)/mode)-1,'type'] = 'Training'
    Feature_df.loc[int(len(basis)/mode)::,'type'] = 'Testing'
        

    Training = Feature_df[Feature_df['type'] == 'Training'][['mean','std']].values

#==========================================================#        
    clf = OneClassSVM(nu=0.2, kernel="rbf", gamma=0.1)
    normalize = preprocessing.scale(Feature_df[['std']].values)
    train = normalize[0:int(len(basis)/mode)]
    test = normalize[int(len(basis)/mode):]
    
    train = np.reshape(train , (len(train),1))
    test = np.reshape(test , (len(test),1))
    Train_and_test = normalize
    clf.fit(train)
#    clf.predict(test)
    predict = clf.predict(Train_and_test)
    Feature_df['feature1_if_abnormal'] = predict
#==========================================================#  
    clf = OneClassSVM(nu=0.2, kernel="rbf", gamma=0.1)    
    normalize = preprocessing.scale(Feature_df[['mean']].values)
    train = normalize[0:int(len(basis)/mode)]
    test = normalize[int(len(basis)/mode):]
    
    train = np.reshape(train , (len(train),1))
    test = np.reshape(test , (len(test),1))
    Train_and_test = normalize
    clf.fit(train)
#    clf.predict(test)
    predict = clf.predict(Train_and_test)
    Feature_df['feature2_if_abnormal'] = predict
 
    Feature_df.loc[ (Feature_df.feature1_if_abnormal == -1) & (Feature_df.feature2_if_abnormal == -1),'if_abnormal'] = -1
    Feature_df.loc[~( (Feature_df.feature1_if_abnormal == -1) & (Feature_df.feature2_if_abnormal == -1)),'if_abnormal'] = 1
    predict = Feature_df['if_abnormal'].values

#==========================================================#      
    up_bound = np.mean(Training[:,0]) + np.std(Training[:,0])
    low_bound = np.mean(Training[:,0]) - np.std(Training[:,0])
    Feature_df.loc[(Feature_df['mean'] > low_bound) & (Feature_df['mean']  < up_bound),'if_abnormal_iqr'] = 1
    Feature_df['if_abnormal_iqr'] = Feature_df['if_abnormal_iqr'].fillna(-1)
#==========================================================#      

    plt.figure()
    plt.scatter(np.arange(len(Feature_df)),Feature_df['std'] , c = predict)
    plt.axvline(int(len(basis)/mode), color='k',linestyle = '--')


    up_bound = max(Feature_df[(Feature_df['feature1_if_abnormal'] ==1) & (Feature_df['type'] =='Training')]['std'])
    low_bound = min(Feature_df[(Feature_df['feature1_if_abnormal'] ==1) & (Feature_df['type'] =='Training')]['std'])
#        up_bound = min(Feature_df[(Feature_df['if_abnormal'] ==-1) & (Feature_df['mean'] > normal_max)]['mean'])
#        low_bound = max(Feature_df[(Feature_df['if_abnormal'] ==-1) & (Feature_df['mean'] < normal_min)]['mean'])
#        up_bound = 1.8297095
#        low_bound = -1.5197436
    plt.axhline(up_bound, color='m',linestyle = '--')
    plt.axhline(low_bound, color='m',linestyle = '--')
    plt.xlabel('period')
    plt.ylabel('std')
    plt.axhline(up_bound, color='m',linestyle = '--',label = 'up_bound')
    plt.axhline(low_bound, color='m',linestyle = '--',label = 'low_bound')
    plt.title('OCSVM')
    plt.legend()
#==========================================================#        
    Feature_df.loc[ (Feature_df.if_abnormal == -1) & (Feature_df.if_abnormal_iqr == -1),'Last_abnormal'] = -1
    Feature_df.loc[~( (Feature_df.if_abnormal == -1) & (Feature_df.if_abnormal_iqr == -1)),'Last_abnormal'] = 1
    predict = Feature_df['Last_abnormal'].values
    
    
    
    
    
    
    
    value['if_abnormal'] = np.nan 
    value['period'] = np.nan 
    source_data = value.copy().reset_index(drop=True)
    for  i in range(int(len(source_data)/mode)):
        source_data.loc[0+i*mode : (i+1)*mode-1,'if_abnormal'] = predict[i]
        source_data.loc[0+i*mode : (i+1)*mode-1,'period'] = int(i)
    
    weekend_median = basis.groupby(['weekdays'],as_index=False)[SourceDataType].mean()

    
    #畫異常部分圖
    source_data = source_data[~source_data['if_abnormal'].isna()]
    normal_part = source_data[source_data['if_abnormal'] == 1]
    abnormal_part = source_data[source_data['if_abnormal'] == -1]
    normal_part1 = pd.merge(normal_part , source_data[['ds']],on = ['ds'],how='right')
    normal_part1 = normal_part1.sort_values(by = ['ds'])
    
    abnormal_part1 = pd.merge(abnormal_part , source_data[['ds']],on = ['ds'],how='right')
    abnormal_part1 = abnormal_part1.sort_values(by = ['ds'])
    
    
    plt.figure(figsize = (20,8))
    plt.plot(source_data['ds'],source_data[SourceDataType],'b')
    plt.plot(normal_part1['ds'],normal_part1[SourceDataType],'b',label = 'Mean Normal')
    plt.plot(abnormal_part1['ds'],abnormal_part1[SourceDataType],'lawngreen',label = 'Mean change')
    plt.axvline(train_cut_date, color='k',linestyle = '--', label = 'train cut')
    plt.title(key)
    plt.xlabel('ds')
    plt.ylabel(SourceDataType)
    plt.xticks(rotation = 90)
    plt.legend()
    plt.savefig('C:/Users/tony_tien/Desktop/tmp_plot/{}.png'.format(key))
    plt.close()
    
 
