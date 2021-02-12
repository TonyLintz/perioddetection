# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 15:46:17 2020

@author: Tony_Tien
"""
import pandas as pd
import numpy as np
import scipy.stats as stats
from config import data_path,SourceDataType,SourceDataType_Small_Name

def Period_calcu_PR_indicator(sip_data):
    
    all_period_eachday_corr = []
    for key2,value2 in sip_data.groupby(['period']):
#        key2 = (3)
#        value2 =sip_data.groupby(['period']).get_group(key2).copy()    
        value2 = value2.sort_values(by = ['weekdays'])
    
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
        a_period_eachday_corr = a_period_all_corr.copy()
        
        corr = (stats.pearsonr(period_list,baseline_list)[0])
        if corr > 0 :
            a_period_eachday_corr1 = (np.array(a_period_eachday_corr) - corr) / corr
        elif (np.isnan(corr)) | (corr == 0):
            a_period_eachday_corr1 = np.ones(len(period_list)) * np.inf
        else:
            a_period_eachday_corr1 = (corr - np.array(a_period_eachday_corr)) / corr
        
        value2['corr_change_rate'] = a_period_eachday_corr1
        all_period_eachday_corr.append(value2)
    all_period_eachday_corr_df = pd.concat(all_period_eachday_corr)
    all_period_eachday_corr_df = all_period_eachday_corr_df.sort_values(by = ['corr_change_rate'],ascending= False).reset_index(drop=True)  
    
    return all_period_eachday_corr_df


def MagnitudeChange_calcu_PR_indicator(sip_data):

    All_period_dt = []
    for period,data in sip_data.groupby(['period']):
    #    period = 47    
    #    data = sip_data.groupby(['period']).get_group((period))
        data = data.sort_values(by = ['weekdays'])
        data['abs_diff'] = abs(data['total_sent_bytes'].values - weekend_median['total_sent_bytes'].values)
        All_period_dt.append(data)
    All_period_dt = pd.concat(All_period_dt)
    All_period_dt = All_period_dt.sort_values(by = ['abs_diff'],ascending= False).reset_index(drop=True)
    
    return All_period_dt
    
    
    