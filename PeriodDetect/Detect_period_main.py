# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 10:34:44 2020

@author: Tony_Tien
"""
import pandas as pd
from loguru import logger
from datetime import datetime

from config import *
from process import create_is_server_column,fill_empty_ds
from whether_have_period import *
from schema.dm import \
    fwip_all_source_private_dest_all_services_port_scan_data

print(parser)
print (("\n"))
print (("SourceDataType is : " + SourceDataType)) 
print (("if_to_current is : " + str(if_to_current))) 

def main():
    try:
        ASUS_Workday = pd.read_csv(work_date_path + 'dim_sc_day_ext.csv',engine='python')[['ds' , 'workday' , 'holiday_type']] 
    except:
        ASUS_Workday = pd.read_csv(work_date_path + 'dim_sc_day_ext.csv',engine='python')[['ds' , 'workday' , 'holiday_type']] 

    from readMultiCSV import CSVAdapter
    csvAdapter = CSVAdapter(
        sep='\t',
        func="",
        NumofThread=8,
        usecols = None,
        DataEncode=['utf8'],
        DataChunkSize=[100000],
        DataSet=[DataSet_reg]
    )
    data = csvAdapter.CollectData(data_folder_path)
    data = data[DataSet_reg]
    data['valid_data'] = 1
    data = data.astype(fwip_all_source_private_dest_all_services_port_scan_data)[['source_ip', 'ds', 'valid_data', SourceDataType]]
    data = data.sort_values(by=["source_ip", "ds"]).reset_index(drop=True)    
    logger.info("data start: {0}, end: {1}".format(data['ds'].min(), data['ds'].max()))
    
    data = data[(data["ds"] >= train_start_date) & (data["ds"] <= test_cut_date)].reset_index(drop=True)
    unique_dest_cnt = data.copy()
    TrainingRawData = unique_dest_cnt[(unique_dest_cnt['ds'] >= train_start_date) & (unique_dest_cnt['ds'] <= train_cut_date)]
    SourceIP_length = TrainingRawData.groupby('source_ip').size().reset_index()
    CanTrainServerIP = SourceIP_length[SourceIP_length[0]>TrainIP_condition_length]['source_ip'].tolist()
    
    unique_dest_cnt1 = unique_dest_cnt[unique_dest_cnt['source_ip'].isin(CanTrainServerIP)]
    unique_dest_cnt1 = unique_dest_cnt1.sort_values(by = ['source_ip','ds'])
    unique_dest_cnt1 = unique_dest_cnt1.groupby(['source_ip']).apply(lambda x: fill_empty_ds(x)).reset_index(drop=True)
    unique_dest_cnt1['weekdays'] = unique_dest_cnt1['ds'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").weekday()+1)
    unique_dest_cnt1 = pd.merge(unique_dest_cnt1 , ASUS_Workday , on = ['ds'] , how='inner')
    TrainData1 = unique_dest_cnt1[(unique_dest_cnt1['ds'] >= train_start_date) & (unique_dest_cnt1['ds'] <= train_cut_date)]
    
    #TrainingData 判斷是否有週期
    Have_Period = detect_have_period(TrainData1,SourceDataType)
    Have_Period = Have_Period[Have_Period['mode'] == mode]
    unique_dest_cnt2 = unique_dest_cnt1[unique_dest_cnt1['source_ip'].isin(Have_Period['sip'].tolist())].copy()
    unique_dest_cnt2.to_csv(data_path + '{}___Have_cycle_source_data.csv'.format(SourceDataType),index=False)
    
    
    
if __name__ == "__main__":
    main()

