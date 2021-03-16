# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 16:39:45 2020

@author: Tony_Tien
"""
from matplotlib import pyplot as plt
import pandas as pd
from config import major_plot_path,sub_plot_path,feature_scatter_path,cycle_general_path,SourceDataType,mode
from process import creat_multi_path
import os
import numpy as np

def plot_normal_abnormal_in_data(source_data,key,flag=True):
  
    if flag:
        
        if os.path.isdir(major_plot_path):

            source_data = source_data[~source_data['if_abnormal'].isna()]
            weekend_holiday_part = source_data[source_data['holiday_type'] == 'weekend-holiday']
            Non_weekend_holiday_part = source_data[source_data['holiday_type'] == 'Non-weekend-holiday']
            fill_date = source_data[source_data['valid_data'] == 0]
            
            normal_part = source_data[source_data['if_abnormal'] == 1]
            abnormal_part = source_data[source_data['if_abnormal'] == -1]
            normal_part1 = pd.merge(normal_part , source_data[['ds']],on = ['ds'],how='right')
            normal_part1 = normal_part1.sort_values(by = ['ds'])
            
            abnormal_part1 = pd.merge(abnormal_part , source_data[['ds']],on = ['ds'],how='right')
            abnormal_part1 = abnormal_part1.sort_values(by = ['ds'])
        
            plt.figure(figsize = (20,8))
            plt.plot(source_data['ds'],source_data[SourceDataType],'b')
            plt.scatter(fill_date['ds'],fill_date[SourceDataType],color = 'm',s=100,label = 'empty_date(fill)')
            plt.scatter(Non_weekend_holiday_part['ds'],Non_weekend_holiday_part[SourceDataType],color = 'goldenrod',s=100,label = 'Non-weekend-holiday')

            plt.plot(normal_part1['ds'],normal_part1[SourceDataType],'b',label = 'Period Normal')
            plt.plot(abnormal_part1['ds'],abnormal_part1[SourceDataType],'r',label = 'Period change')
            plt.title(key)
            plt.xlabel('ds')
            plt.ylabel(SourceDataType)
            plt.xticks(rotation = 90)
            plt.legend()
            plt.savefig(major_plot_path + '{}.png'.format(key))
            plt.close()
        else:
            creat_multi_path(major_plot_path)
            plot_normal_abnormal_in_data(source_data,key,flag=True)
    else:
        pass


def plot_normal_abnormal_period(Feature_df, source_data, key, flag=True):
    if flag:
        creat_multi_path(os.path.join(sub_plot_path,f'{key}/normal_weekday/'))
        creat_multi_path(os.path.join(sub_plot_path,f'{key}/abnormal_weekday/'))
        normal = Feature_df[Feature_df['if_abnormal'] == 1]
        
        for i in Feature_df.index:
            
            if i in normal.index:
                pltfile_name = 'normal_weekday'
            else:
                pltfile_name = 'abnormal_weekday'
            
            aa = source_data[source_data['period'] == i]
            aa = aa.sort_values(by = ['weekdays'])
            holiday_df = aa[aa['holiday_type'] == 'Non-weekend-holiday']
            empty_df = aa[aa['valid_data'] == 0]
            
            plt.plot(aa['weekdays'],aa[SourceDataType])
            plt.scatter(holiday_df['weekdays'],holiday_df[SourceDataType],color= 'goldenrod',s=100,label='Non-weekend-holiday')
            plt.scatter(empty_df['weekdays'],empty_df[SourceDataType],color = 'm',s=100,label='empty_date(fill)')
            plt.title('{}_th period'.format(i))
            plt.xlabel('weekday')
            plt.ylabel(SourceDataType)
            plt.legend()
            #畫圖路徑
            draw_path = sub_plot_path + '/{}/{}/'.format(key,pltfile_name)    
            plt.savefig(draw_path + '{}_th period.png'.format(i))
            plt.close()
    else:
        pass
            
    

def plot_feature_scatter(basis, predict_ocsvm, Feature_df_ocsvm, key, flag=True):
    if flag:
        creat_multi_path(os.path.join(feature_scatter_path,f'{key}/'))
        plt.figure()
        plt.scatter(np.arange(len(Feature_df_ocsvm)),Feature_df_ocsvm['FFT_Amp'] , c = predict_ocsvm)
        plt.axvline(Feature_df_ocsvm.loc[Feature_df_ocsvm.type == 'Training'].iloc[-1].name, color='k',linestyle = '--')

        try:
            up_bound = max(Feature_df_ocsvm[(Feature_df_ocsvm['if_abnormal'] ==1) & (Feature_df_ocsvm['type'] =='Training')]['FFT_Amp'])
            low_bound = min(Feature_df_ocsvm[(Feature_df_ocsvm['if_abnormal'] ==1) & (Feature_df_ocsvm['type'] =='Training')]['FFT_Amp'])
            plt.axhline(up_bound, color='m',linestyle = '--',label = 'up_bound')
            plt.axhline(low_bound, color='m',linestyle = '--',label = 'low_bound')

        except:
            pass

        plt.ylabel('feature point')
        plt.xlabel('week')
        draw_path = os.path.join(feature_scatter_path,f'{key}/')
        plt.savefig(draw_path + f'{key}.png')
        plt.close()
    else:
        pass
    
    
def plot_cycle_general(SourceDataType, weekend_median, key, flag=True):
    if flag:
        creat_multi_path(os.path.join(cycle_general_path,f'{key}/'))
        plt.figure()
        plt.title(key + '_median')
        plt.bar(weekend_median['weekdays'],weekend_median['median_of_cycle'])
        plt.ylabel(SourceDataType)
        plt.xlabel('weekdays')
        
        draw_path = os.path.join(cycle_general_path,f'{key}/')
        plt.savefig(draw_path + f'{key}.png')
        plt.close()
    else:
        pass