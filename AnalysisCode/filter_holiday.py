# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 17:03:47 2020

@author: Tony_Tien
"""

from matplotlib import pyplot as plt

with open(pickle_path + f"{model_name}.pkl", 'rb') as f:
    All_Model_InfoDict = pickle.load(f)

with open(pickle_path + f"{feature_set_name}.pkl", 'rb') as f:
    All_SIP_FEATURE_DICT = pickle.load(f)


unique_dest_cnt2 = pd.read_csv(data_path + 'Have_cycle_source_data.csv')

Result_File = pd.read_csv('../data/total_sent_bytes___Result_File.csv')[['source_ip', 'ds' ,'if_abnormal']]
unique_dest_cnt2.loc[(unique_dest_cnt2.weekend.isin([6,7])),'holiday_type'] = 'weekend-holiday'
unique_dest_cnt2.loc[(unique_dest_cnt2.weekend.isin([1,2,3,4,5])) & (unique_dest_cnt2.workday == 0) ,'holiday_type'] = 'Non-weekend-holiday'
unique_dest_cnt2['holiday_type'] = unique_dest_cnt2['holiday_type'].fillna('workday')
unique_dest_cnt3 = pd.merge(unique_dest_cnt2 , Result_File , on=['source_ip','ds'])

for key ,value in unique_dest_cnt3.groupby(['source_ip']):
#     key = '192.168.81.141'
    
    Feature_df = All_SIP_FEATURE_DICT[key]
    mode = 7
    basis = value[(value['ds'] >= train_start_date) & (value['ds'] <= train_cut_date)]
    
    Predict = Modeling_predict(Feature_df.loc[:,'FFT_Amp'].values,model)
    Feature_df['if_abnormal'] = Predict
    Feature_df = transform_label(Feature_df)
#    Feature_df = Resurrection(Feature_df)
    
    source_data = unique_dest_cnt3.groupby(['source_ip']).get_group(key).copy()
    abnormal_part = source_data[source_data['if_abnormal'] == -1]
    abnormal_part1 = pd.merge(abnormal_part , source_data[['ds']],on = ['ds'],how='right')
    abnormal_part1 = abnormal_part1.sort_values(by = ['ds'])
    
    source_data = source_data[~source_data['holiday_type'].isna()]
    workday_part = source_data[source_data['holiday_type'] == 'workday']
    weekend_holiday_part = source_data[source_data['holiday_type'] == 'weekend-holiday']
    Non_weekend_holiday_part = source_data[source_data['holiday_type'] == 'Non-weekend-holiday']
    

    plt.figure(figsize = (20,8))
    plt.plot(source_data['ds'],source_data[SourceDataType],'b')
    plt.scatter(weekend_holiday_part['ds'],weekend_holiday_part[SourceDataType],color = 'm',s=100,label = 'weekend-holiday')
    plt.scatter(Non_weekend_holiday_part['ds'],Non_weekend_holiday_part[SourceDataType],color = 'goldenrod',s=100,label = 'Non-weekend-holiday')
#    plt.plot(abnormal_part1['ds'],abnormal_part1['total_sent_bytes'],'r',label = 'Period change')

    plt.title(key)
    plt.xlabel('ds')
    plt.ylabel(SourceDataType)
    plt.xticks(rotation = 90)
    plt.legend()
    plt.savefig('C:/Users/tony_tien/Desktop/tmp_plot1/' + '{}.png'.format(key))
    plt.close()
    
    
    
    mode = 7
    source_data['period'] = np.nan
    source_data = source_data.reset_index(drop=True)
    for  i in range(int(len(source_data)/mode)):
        source_data.loc[0+i*mode : (i+1)*mode,'period'] = int(i)

    plot_normal_abnormal_period(Feature_df,source_data,key,flag=True)
