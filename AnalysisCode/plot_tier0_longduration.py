# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:27:38 2021

@author: Tony_Tien
"""

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.font_manager import FontProperties

focus_source = 'serverrst_ratio'
focus_pr = 'sr_peak_pr'
focus_predict = 'sr_peak_pred'

path = 'P:/Workspace/visulization_by_bokeh/VisulizationByBokeh_firewalltrafficabnormal/Data/'
merge_pred_and_indicator = pd.read_csv(path + 'merge_pred_and_indicator_20210125_rst.csv')
merge_pred_and_indicator1 = merge_pred_and_indicator[['source_ip', 'ds', 'weekdays','holiday_type','description','tier0',\
                                                      'long_duration','serverrst_ratio_sub', 'clientrst_ratio_sub','clientrst_ratio','serverrst_ratio','cr_peak_pred',\
                                                      'cr_peak_pr','sr_peak_pred', 'sr_peak_pr']]
#找tier0 and !=long_duration and sr_peak_pr=95up or cr_peak_pr = 95up &
ds_range = merge_pred_and_indicator1[(merge_pred_and_indicator1['ds'] >= '2020-12-01') & (merge_pred_and_indicator1['ds'] <= '2021-01-18')]
ds_range_tier0_nolongduration = ds_range[(ds_range['tier0'] == False) & (ds_range['long_duration'] == False)]
ds_range_tier0_nolongduration_pred = ds_range_tier0_nolongduration[ds_range_tier0_nolongduration[focus_predict] == 2]
ds_range_tier0_nolongduration_pred1 = ds_range_tier0_nolongduration_pred[ds_range_tier0_nolongduration_pred[focus_pr] > 90]

ss = merge_pred_and_indicator1[merge_pred_and_indicator1['source_ip'].isin(ds_range_tier0_nolongduration_pred1['source_ip'].unique())]
ss[focus_source] = ss[focus_source]/100

for sip,value in ss.groupby(['source_ip']):
#    print(value)
#    sip = '172.20.112.1'
    value = ss.groupby(['source_ip']).get_group(sip)
    bad_abnormal_date = value[(value[focus_predict] == 2) & (value[focus_pr] > 90)]
    abnormal_date = value[(value[focus_predict] == 2) & (value[focus_pr] <= 90)]
    
#    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei'] 
#    plt.rcParams['axes.unicode_minus'] = False
    plt.figure(figsize=(20,6))
    plt.plot(value['ds'],value[focus_source])
    plt.plot(bad_abnormal_date['ds'],bad_abnormal_date[focus_source],'ro',label = 'PR > 90')
    plt.plot(abnormal_date['ds'],abnormal_date[focus_source],'bo',label = 'PR < 90')
    plt.xlabel('ds',fontsize = 16)
    plt.xticks(rotation=70)
    plt.ylabel(focus_source,fontsize = 16)
    plt.title('source ip = {}'.format(sip))
    plt.legend()
    plt.savefig('C:/Users/tony_tien/Desktop/tmp_plot3/{}.png'.format(sip))
    plt.close()


