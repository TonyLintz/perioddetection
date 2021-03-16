# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 17:26:46 2020

@author: Tony_Tien
"""

import pandas as pd
import numpy as np
import scipy.stats as stats
import math 
from config import data_path,SourceDataType,SourceDataType_Small_Name

total_sent_bytes___Result_File = pd.read_csv(f'D:/GitProject/perioddetection/data/period_{SourceDataType_Small_Name}_result_file.csv')
total_sent_bytes___Result_File = total_sent_bytes___Result_File.sort_values(by = ['source_ip','ds'])


#Rank Percetage
All_sip_percetage = pd.DataFrame()
for key, value in total_sent_bytes___Result_File.groupby(['source_ip']):
#    key = ('172.16.98.136')
#    value = total_sent_bytes___Result_File.groupby(['source_ip']).get_group(key)    
    
    all_period_eachday_corr = []
    for key2,value2 in value.groupby(['period']):
#        key2 = (3)
        value2 =value.groupby(['period']).get_group(key2).copy()    
        value2 = value2.sort_values(by = ['weekend'])
    
        period_list = value2[SourceDataType].values
        baseline_list = value2['median_of_cycle'].values
        
        a_period_all_corr = []
        for i in range(0,7):
            period_list_pop = np.delete(period_list , i)
            baseline_list_pop = np.delete(baseline_list , i)
            coeffi = stats.pearsonr(period_list_pop,baseline_list_pop)[0]
            if np.isnan(coeffi):
                a_period_all_corr.append(0)
            else:  
                a_period_all_corr.append(coeffi)
        
        a_period_eachday_corr = []
        for i in range(0,7):
            a_period_all_corr_pop = np.delete(np.array(a_period_all_corr) , i)
            a_period_eachday_corr.append(np.max(a_period_all_corr_pop))
        
        value2['corr_score'] = a_period_eachday_corr
        all_period_eachday_corr.append(value2)
    kk = pd.concat(all_period_eachday_corr)
    kk['corr_score'] = 1 - kk['corr_score']
    kk = kk.sort_values(by = ['corr_score'],ascending= False).reset_index(drop=True)
    kk['Corr_Rank'] = kk['corr_score'].rank(method='min')
    kk['Corr_Percentage'] = (kk['Corr_Rank'] - 1) / max(kk['Corr_Rank'])
    kk['Corr_Percentage'] = np.round(kk['Corr_Percentage'] , 3)
    kk['Corr_Percentage'] = list(map(lambda x: math.floor(x),(kk['Corr_Percentage'].values) *100))
    All_sip_percetage = pd.concat([All_sip_percetage , kk])
All_sip_percetage.to_csv(data_path + f'period_{SourceDataType_Small_Name}_rank_percentile.csv')




#PR

abnormal_data = total_sent_bytes___Result_File[total_sent_bytes___Result_File['if_abnormal'] == -1]
 
    
all_period_eachday_corr = []
for key2,value2 in abnormal_data.groupby(['source_ip','period']):
#        key2 = ('172.16.98.87',13)
    value2 = total_sent_bytes___Result_File.groupby(['source_ip','period']).get_group(key2).copy()    
    value2 = value2.sort_values(by = ['weekend'])

    period_list = value2[SourceDataType].values
    baseline_list = value2['median_of_cycle'].values*1048576
    
    a_period_all_corr = []
    for i in range(0,7):
        period_list_pop = np.delete(period_list , i)
        baseline_list_pop = np.delete(baseline_list , i)
        coeffi = stats.pearsonr(period_list_pop,baseline_list_pop)[0]
        if np.isnan(coeffi):
            a_period_all_corr.append(0)
        else:  
            a_period_all_corr.append(coeffi)
    a_period_eachday_corr = a_period_all_corr.copy()
    corr = (stats.pearsonr(period_list,baseline_list)[0])
    (np.array(a_period_eachday_corr)-corr) * (1-corr)
#    a_period_eachday_corr = []
    for i in range(0,7):
        a_period_all_corr_pop = np.delete(np.array(a_period_all_corr) , i)
        a_period_eachday_corr.append(np.max(a_period_all_corr_pop))
    
    value2['corr_score'] = a_period_eachday_corr
    all_period_eachday_corr.append(value2)
    
kk = pd.concat(all_period_eachday_corr)
kk['corr_score'] = 1 - kk['corr_score']
kk = kk.sort_values(by = ['corr_score'],ascending= False).reset_index(drop=True)
kk["PR"] = kk.index
kk["PR"] = (len(kk) - kk["PR"] - 1) / len(kk) * 100
kk["PR"] = np.floor(kk["PR"]).astype(int)

kk.to_csv(data_path + f'period_{SourceDataType_Small_Name}_PR.csv')
    

aaa = kk[kk['period'] == 20]

plt.plot(np.arange(len(baseline_list_pop)),period_list_pop,label = 'test')
plt.plot(np.arange(len(baseline_list_pop)),baseline_list_pop,label = 'baseline')
plt.xticks(np.arange(len(baseline_list_pop)) , [1,2,3,5,6,7])
plt.legend()
