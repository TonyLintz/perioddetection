# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 15:44:55 2020

@author: Tony_Tien
"""

from matplotlib import pyplot as plt
SourceDataType = 'total_sent_bytes'
Have_cycle_source_data = pd.read_csv(data_path + '{}___Have_cycle_source_data.csv'.format(SourceDataType))
mode = 7

for key,value in Have_cycle_source_data.groupby(['source_ip']):
    
#   key = '172.21.132.173'
    value = Have_cycle_source_data.groupby(['source_ip']).get_group(key)
    value = value.reset_index(drop=True)
#    value.loc[value.is_server == '0',SourceDataType] = np.nan
    plt.figure(figsize = (20,8))
    for x1,x2,y1,y2 in zip(range(len(value)) ,range(len(value))[1:] , value[SourceDataType].values  , value[SourceDataType].values[1:]):
        if value.loc[x2,'is_server'] == '0':
            p1 = plt.plot([x1, x2], [y1, y2], 'r')
        else:
            p2 = plt.plot([x1, x2], [y1, y2], 'b')     
    plt.legend(['rawdata', 'missing value'])
    plt.title(key)
    plt.xlabel('time_index')
    plt.ylabel(SourceDataType)
    plt.savefig(f'C:/Users/tony_tien/Desktop/tmp_plot1/' + f'{key}.png')
    plt.close()
    
    creat_multi_path(f'C:/Users/tony_tien/Desktop/tmp_plot2/{key}/')
    for  i in range(int(len(value)/mode)):
      
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        holiday_df = test_data_raw[test_data_raw['is_server'] == '0']
        plt.plot(test_data_raw['weekdays'],test_data_raw[SourceDataType],marker = 'o')
        plt.scatter(holiday_df['weekdays'],holiday_df[SourceDataType],color= 'goldenrod',s=200,label='Non-weekend-holiday')
        plt.title('{}_th period'.format(i))
        plt.xlabel('weekday')
        plt.ylabel(SourceDataType)
        plt.savefig(f'C:/Users/tony_tien/Desktop/tmp_plot2/{key}/' + '{}_th period.png'.format(i))
        plt.close()
        
not_nan = value[~value[SourceDataType].isna()]
basis_filter_hday = not_nan[(not_nan['ds'] >= train_start_date) & (not_nan['ds'] <= train_cut_date)]
    
weekend_median = not_nan.groupby(['weekdays'],as_index=False)[SourceDataType].median().rename(columns = {SourceDataType:'median_of_cycle'})
ff = not_nan.groupby(['weekdays'],as_index=False).get_group(6)
ff = not_nan.groupby(['weekdays'],as_index=False).get_group(7)

plt.figure()
plt.title(key + '_median')
plt.bar(weekend_median['weekdays'],weekend_median['median_of_cycle'])
plt.ylabel(SourceDataType)
plt.xlabel('weekdays')
   


#=================================================================================#
sip_size = Have_cycle_source_data.groupby(['source_ip']).size().reset_index()
sip_size['period_size'] = np.floor(sip_size[0] / 7)   
fill_empty = Have_cycle_source_data[Have_cycle_source_data['is_server'] == '0']
aaa = fill_empty.groupby(['source_ip','weekdays']).size().reset_index().rename(columns = {0:'empty_period_num'})

aaaa = pd.merge(aaa,sip_size[['source_ip','period_size']] , on = ['source_ip'])
aaaa['empty_period_ratio'] = aaaa['empty_period_num'] / aaaa['period_size']

plt.boxplot(aaaa['empty_period_ratio'])
bbb = aaaa[(aaaa['empty_period_ratio'] > 0.5) & (aaaa['empty_period_ratio'] < 0.9)]


aaaa.loc[aaaa.weekdays.isin([6,7]),'is_weekend'] = 1
aaaa.loc[aaaa.weekdays.isin([1,2,3,4,5]),'is_weekend'] = 0 

is_weekend = aaaa[aaaa['is_weekend'] == 1]
No_weekend = aaaa[aaaa['is_weekend'] == 0]

plt.boxplot([is_weekend.empty_period_num.values , No_weekend.empty_period_num.values])
#===============================================================================#


All_SIP_Fill_Data = pd.DataFrame()
for key,value in Have_cycle_source_data.groupby(['source_ip']):    
#    key = '172.20.110.187'
    value = Have_cycle_source_data.groupby(['source_ip']).get_group(key)
    value = value.reset_index(drop=True)
    value.loc[value.is_server == '0',SourceDataType] = np.nan
    not_nan = value[~value[SourceDataType].isna()]
    
    fill_empty = value[value['is_server'] == '0']
    all_fill_0_weekdays = fill_empty.groupby(['source_ip','weekdays']).size().reset_index().rename(columns = {0:'empty_period_num'})
    all_fill_0_weekdays['empty_period_ratio'] = all_fill_0_weekdays['empty_period_num'] / int(len(value)/mode)
    all_fill_0_weekdays = all_fill_0_weekdays[all_fill_0_weekdays['empty_period_ratio'] > 0.9]['weekdays'].tolist()
    
    weekend_median = not_nan.groupby(['weekdays'],as_index=False)[SourceDataType].median().rename(columns = {SourceDataType:'median_of_cycle'})
    weekend_median = pd.merge(value[['weekdays']].drop_duplicates() , weekend_median , on =['weekdays'],how='left')
    weekend_median.loc[weekend_median.weekdays.isin(all_fill_0_weekdays) , 'median_of_cycle'] = 0
    weekend_median = weekend_median.sort_values(by = ['weekdays'])
    
    All_period = pd.DataFrame()
    for i in range(int(len(value)/mode)):
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        test_data_raw_filter_na = test_data_raw[test_data_raw['is_server'] != '0'] #不用補植的人
        
        if (len(test_data_raw_filter_na) != mode) & (len(test_data_raw_filter_na) != 0):
            weekend_median_filter = weekend_median[weekend_median['weekdays'].isin(test_data_raw_filter_na['weekdays'])]
            try:
                ratio = ( (test_data_raw_filter_na[SourceDataType].mean()) / (weekend_median_filter['median_of_cycle'].mean()) )        
            except:
                ratio = 1
                
            for weekday in set(value['weekdays'].values):
                if weekday in all_fill_0_weekdays:
                    test_data_raw.loc[(test_data_raw.is_server == '0') & (test_data_raw.weekdays == weekday),SourceDataType] = 0
                else:
                    test_data_raw.loc[(test_data_raw.is_server == '0') & (test_data_raw.weekdays == weekday),SourceDataType] = \
                    weekend_median.loc[weekend_median.weekdays == weekday,'median_of_cycle'].values[0] * ratio
        elif len(test_data_raw_filter_na) == 0:
            test_data_raw.loc[:,SourceDataType] = weekend_median.loc[:,'median_of_cycle'].values
        else:
            pass
        
        All_period = All_period.append(test_data_raw)

    All_period = All_period.sort_values(by = 'ds')
    All_SIP_Fill_Data = All_SIP_Fill_Data.append(All_period)
All_SIP_Fill_Data = All_SIP_Fill_Data.sort_values(by= ['source_ip', 'ds'])
All_SIP_Fill_Data.to_csv(data_path + '{}___Have_cycle_source_data(fillvalue).csv'.format(SourceDataType),index=False)

    
    New_value = All_period.copy()
    New_value['DataToLog'] =  np.log1p(New_value[SourceDataType]) 

    creat_multi_path(f'C:/Users/tony_tien/Desktop/tmp_plot/{key}/')
    for  i in range(int(len(New_value)/mode)):
      
        test_data_raw = New_value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        holiday_df = test_data_raw[test_data_raw['is_server'] == '0']
        plt.plot(test_data_raw['weekdays'],test_data_raw[SourceDataType],marker = 'o')
        plt.scatter(holiday_df['weekdays'],holiday_df[SourceDataType],color= 'goldenrod',s=200,label='Non-weekend-holiday')
        plt.title('{}_th period'.format(i))
        plt.xlabel('weekday')
        plt.ylabel(SourceDataType)
        plt.savefig(f'C:/Users/tony_tien/Desktop/tmp_plot/{key}/' + '{}_th period.png'.format(i))
        plt.close()

    


New_value['DataToLog'] =  np.log10(1+New_value[SourceDataType])   
SourceDataType = 'DataToLog'
plt.figure(figsize = (20,8))
for x1,x2,y1,y2 in zip(range(len(New_value)) ,range(len(New_value))[1:] , New_value[SourceDataType].values  , New_value[SourceDataType].values[1:]):
    if New_value.loc[x2,'is_server'] == '0':
        p1 = plt.plot([x1, x2], [y1, y2], 'r' )
    else:
        p2 = plt.plot([x1, x2], [y1, y2], 'b' )   
leg = plt.legend(['interpolation','rawdata'], fontsize = 16)
leg.legendHandles[0].set_color('red')
leg.legendHandles[1].set_color('blue')

plt.title(key)
plt.xlabel('time_index' , fontsize = 16)
plt.ylabel(SourceDataType , fontsize = 16)
plt.xticks(fontsize = 16)
plt.yticks(fontsize = 20)
plt.savefig(f'C:/Users/tony_tien/Desktop/tmp_plot1/' + f'{key}.png')
plt.close()
    

    
    
holiday_df = test_data_raw[test_data_raw['is_server'] == '0']    
plt.plot(test_data_raw['weekdays'],test_data_raw[SourceDataType],marker = 'o')
plt.scatter(holiday_df['weekdays'],holiday_df[SourceDataType],color= 'goldenrod',s=200,label='Non-weekend-holiday')
plt.title('{}_th period'.format(i))
plt.xlabel('weekday')
plt.ylabel(SourceDataType)














