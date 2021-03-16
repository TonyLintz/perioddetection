# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 11:56:57 2020

@author: Tony_Tien
"""

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import math
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

All_period_dt = []
for period,data in source_data.groupby(['period']):
#    period = 47    
    data = source_data.groupby(['period']).get_group((period))
    data = data.sort_values(by = ['weekdays'])
    data['abs_diff'] = abs(data['total_sent_bytes'].values - weekend_median['total_sent_bytes'].values)
    All_period_dt.append(data)
All_period_dt = pd.concat(All_period_dt)
All_period_dt = All_period_dt.sort_values(by = ['abs_diff'],ascending= False).reset_index(drop=True)    
All_period_dt['Diff_Rank'] = All_period_dt['abs_diff'].rank(method='min')
All_period_dt['Rank_Percentage'] = (All_period_dt['Diff_Rank'] - 1) / max(All_period_dt['Diff_Rank'])
All_period_dt['Rank_Percentage'] = np.round(All_period_dt['Rank_Percentage'] , 3)
All_period_dt['Rank_Percentage'] = list(map(lambda x: math.floor(x),(All_period_dt['Rank_Percentage'].values) *100))
All_period_dt = All_period_dt.sort_values(by = ['ds'])


for key,value in All_period_dt.groupby(['period']):
    value = All_period_dt.groupby(['period']).get_group(key)
    value = value.sort_values(by = ['weekdays']).reset_index(drop=True)
    holiday_df = value[value['holiday_type'] == 'Non-weekend-holiday']
    
    
    plt.bar(weekend_median['weekdays'],weekend_median[SourceDataType],alpha = 0.5,label = 'Train basis')
    plt.bar(value['weekdays'],value[SourceDataType],alpha = 0.5 , color = 'orange',label = 'this period')
#    plt.scatter(holiday_df['weekdays'],holiday_df[SourceDataType],color= 'goldenrod',s=100,label='Non-weekend-holiday')
    for i in range(len(value)):
        if value['Rank_Percentage'].loc[i] >= 95:
            plt.text(value.loc[i,'weekdays'], value.loc[i,'total_sent_bytes'] + (1/10)*np.std(value['total_sent_bytes']) , value['Rank_Percentage'].loc[i], fontsize=14,color = 'r')
        else:
            plt.text(value.loc[i,'weekdays'], value.loc[i,'total_sent_bytes'] + (1/10)*np.std(value['total_sent_bytes']) , value['Rank_Percentage'].loc[i], fontsize=14)

    plt.title('{}_th period'.format(key))
    plt.xlabel('weekdays')
    plt.ylabel(SourceDataType)
    plt.legend()
    plt.savefig('C:/Users/tony_tien/Desktop/tmp_plot2/{}_th period.png'.format(int(key)))
    plt.close()
    