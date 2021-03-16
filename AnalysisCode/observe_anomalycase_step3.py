# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 11:16:38 2021

@author: Tony_Tien
"""
import pandas as pd
import numpy as np
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 1000)

date = 20200921

data_path = 'J:/Data_Anomaly_Detection/raw/fwip_serverfarm_source_dest/'
file_list = os.listdir(data_path)
file_list1 = list(map(lambda x: x.split('_')[1],file_list))
file_list1 = sorted(file_list1)
file_list2 = list(filter(lambda x: (x >= (str(date)+'.csv')) & (x <= (str(date)+'.csv')) ,file_list1))
ko = list(filter(lambda x: (x.split('_')[1]) in (list(set(file_list2))),file_list))
ko = sorted(ko)


from tqdm import tqdm
all_df = []
for file in tqdm(ko):
    try:
        df = pd.read_csv(data_path + file , low_memory = True)
        all_df.append(df)
    except:
        pass
all_df_Df = pd.concat(all_df)

source_ip = all_df_Df[(all_df_Df['source_ip'] == '172.22.44.54') & (all_df_Df['ds'] == date)]
source_ip = source_ip.sort_values(by = ['action','source_ip','ds','dest_ip','dest_port'])
source_ip = source_ip[['source_ip','dest_ip','dest_port','action','total_sent_bytes','ds']]
source_ip['action'].value_counts()
source_ip.groupby(['action'])['total_sent_bytes'].sum()

deny = source_ip[source_ip['action'] == 'deny']
source_ip['total_sent_bytes'].sum()/(1024*1024)
ass = source_ip[source_ip['total_sent_bytes'] > 1000000]
bss = source_ip[source_ip['total_sent_bytes'] > 1000000]

all_df_Df1 = pd.concat(all_df)
source_ip1 = all_df_Df1[(all_df_Df1['source_ip'] == '192.168.77.148') & (all_df_Df1['ds'] == 20200903)]
source_ip1 = source_ip1.sort_values(by = ['source_ip','ds','dest_ip','dest_port'])
source_ip1 = source_ip1[['source_ip','dest_ip','dest_port','action','total_sent_bytes','ds']]
source_ip1['total_sent_bytes'].sum() /(1024*1024)
len(source_ip1[source_ip1['total_sent_bytes'] > 1000000])


source_ip = all_df_Df[(all_df_Df['source_ip'] == '172.22.4.15') & (all_df_Df['ds'] == 20201101)]
source_ip = source_ip.sort_values(by = ['source_ip','ds','dest_ip','dest_port'])
source_ip = source_ip[['source_ip','dest_ip','dest_port','action','total_sent_bytes','ds']]
source_ip['total_sent_bytes'].sum()


source_ip = all_df_Df[(all_df_Df['source_ip'] == '172.22.4.15') & (all_df_Df['ds'] == 20201025)]
source_ip = source_ip.sort_values(by = ['source_ip','ds','dest_ip','dest_port'])
source_ip = source_ip[['source_ip','dest_ip','dest_port','action','total_sent_bytes','ds']]
source_ip['total_sent_bytes'].sum()/(1024*1024)
