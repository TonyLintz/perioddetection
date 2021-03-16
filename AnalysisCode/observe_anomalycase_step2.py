# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 15:36:59 2021

@author: Tony_Tien
"""

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 1000)

from matplotlib import pyplot as plt
key = '172.16.98.130'
All_period_dt = aaa[aaa['source_ip'] == key]
SourceDataType = 'clientrst_ratio_sub'


for key,value in All_period_dt.groupby(['period']):
    key = 31
    value = All_period_dt.groupby(['period']).get_group(key)
    value = value.sort_values(by = ['weekdays']).reset_index(drop=True)
    holiday_df = value[value['holiday_type'] == 'Non-weekend-holiday']
    empty_df = value[value['serverrst_ratio'].isna()]
    #    plt.bar(weekend_median['weekdays'],weekend_median[SourceDataType],alpha = 0.5,label = 'Train basis')
    plt.plot(value['weekdays'],value[SourceDataType],alpha = 0.5 , color = 'blue',label = 'this period')
    plt.scatter(holiday_df['weekdays'],holiday_df[SourceDataType],color= 'goldenrod',s=100,label='Non-weekend-holiday')
    plt.scatter(empty_df['weekdays'],empty_df[SourceDataType],color= 'm',s=100,label='empty_date(fill)')

    for i in range(len(value)):
        if value['tsm_period_pr'].loc[i] >= 80:
            plt.text(value.loc[i,'weekdays'], value.loc[i,SourceDataType] + (1/10)*np.std(value[SourceDataType]) , value['sr_period_pr'].loc[i], fontsize=14,color = 'r')
        else:
            plt.text(value.loc[i,'weekdays'], value.loc[i,SourceDataType] + (1/10)*np.std(value[SourceDataType]) , value['sr_period_pr'].loc[i], fontsize=14)

    plt.title('{}_th period'.format(key))
    plt.xlabel('weekdays')
    plt.ylabel(SourceDataType)
    plt.legend()
    plt.savefig('C:/Users/tony_tien/Desktop/tmp_plot2/{}_th period.png'.format(int(key)))
    plt.close()
    
