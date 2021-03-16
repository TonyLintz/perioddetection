# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 10:35:09 2021

@author: Tony_Tien
"""
import os
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.font_manager import FontProperties
from tqdm import tqdm
from datetime import datetime
from matplotlib import pyplot as plt


accesskey_id = 'LTAI4G2Dv1akuexHeKszNDu7'
accesskey_secret = 'ZtYtiTDIWStOHWnHpKiuMgR53vhQjn'
project = 'dp_infra_dev'
endpoint = 'http://service.ap-southeast-1.maxcompute.aliyun.com/api'


sql_cmd =\
"""SELECT 
source_ip,dest_ip,dest_port,
CONCAT(SUBSTR(ds, 1, 4), '-', SUBSTR(ds, 5, 2),'-' , SUBSTR(ds, 7, 2)) AS ds,
action,
total_sent_bytes,
record_count
FROM dp_infra.dws_infra_net_srcip_destip_destport_action_app_fwip_count
WHERE ds >= 20210204 AND ds <= 20210204 AND source_ip = '172.22.36.179';
"""

from odps import ODPS
from odps import options
from odps.models.record import Record

class AliODPS():
    def __init__(self, accesskey_id, accesskey_secret, project, endpoint):
        self.accesskey_id = accesskey_id
        self.accesskey_secret = accesskey_secret
        self.project = project
        self.endpoint = endpoint
        
    def connect(self):
        self.odps = ODPS(self.accesskey_id ,
                         self.accesskey_secret,
                         self.project,
                         self.endpoint)

    def create_table(self, table_schema):
        self.odps.execute_sql(table_schema)

    def execute_sql(self, sql_cmd):
        self.odps.execute_sql(sql_cmd)

    def write_table(self, table_name, file_path, csv_column_name):
        raw_data = pd.read_csv(file_path + ".csv", encoding='utf8',
                           sep=',', dtype='unicode',
                           skiprows=1, header=None,
                           keep_default_na=False)
        raw_data.columns = csv_column_name
        raw_data = raw_data.replace(np.nan, '', regex=True)
        data_listobject = raw_data.values.tolist()
        self.odps.write_table(table_name, data_listobject)

    def get_data(self, sql_command):
        result = {}
        with self.odps.execute_sql(sql_command).open_reader(tunnel=True) as reader:
            for record in reader:
                for item in record:
                    if item[0] not in result:
                        result[item[0]] = []
                    result[item[0]].append(item[1])
        return result

    def drop_table(self, table_name):
        sql_format = 'DROP TABLE IF EXISTS {table_name}'
        self.odps.execute_sql(sql_format.format(table_name=table_name))

    def truncate_table(self, table_name):
        sql_format = 'TRUNCATE TABLE {table_name};'
        self.odps.execute_sql(sql_format.format(table_name=table_name))

def get_execute_sql(odps, sql_command):
    result = odps.get_data(sql_command)
    df = pd.DataFrame(result) 
    return df


odps = AliODPS(accesskey_id, accesskey_secret, project, endpoint)
odps.connect()
df = get_execute_sql(odps, sql_cmd)
df['weekdays'] = df['ds'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").weekday()+1)

#===========================分析tag================================#

focus_source = 'serverrst_ratio'
focus_pr = 'tsm_period_pr'
focus_predict = 'tsm_period_pred'

path = 'J:/Data_Anomaly_Detection/ap/for_bokeh/'
merge_pred_and_indicator = pd.read_csv(path + 'merge_pred_and_indicator_20210201.csv')
period_data = merge_pred_and_indicator[~merge_pred_and_indicator['tsm_period_pred'].isna()]

abnormal_ = period_data[(period_data[focus_pr] > 80) & (period_data[focus_predict] == 1)]
abnormal_recent = abnormal_[(abnormal_['source_ip'] == '172.28.33.36')]
abnormal_recent = abnormal_recent[(abnormal_recent['ds'] >= '2020-11-01') & (abnormal_recent['ds'] <= '2021-02-01')]
abnormal_recent = abnormal_recent[['source_ip','ds','weekdays','holiday_type','period',focus_pr,focus_predict,'total_sent_mb_sub']]




this_weekdays = df[df['weekdays'] == abnormal_recent['weekdays'].values[0]]

#異常日期 與 過往dest_ip數量中位數的差 
All_destip_num = []
for ds,data in this_weekdays.groupby(['ds']):
#    ds = '2021-01-17'
    data = this_weekdays.groupby(['ds']).get_group(ds)
    
    #dest_ip數量
    dest_ip_num = len(set(data['dest_ip']))
    All_destip_num.append(dest_ip_num)
    
destip_num_median = np.median(All_destip_num)
abnormal_date_data = this_weekdays.groupby(['ds']).get_group(abnormal_recent['ds'].values[0])    
with_median_diff = abs(destip_num_median - len(set(abnormal_date_data['dest_ip'])))


#異常日期下使用的dest ip 與 過往使用過dest_ip的涵蓋率
this_weekdays_filter_abnor = this_weekdays[this_weekdays['ds'] < abnormal_recent['ds'].values[0]]
Coverage_rate = len(set(this_weekdays_filter_abnor['dest_ip']) & set(abnormal_date_data['dest_ip'])) / len(set(this_weekdays_filter_abnor['dest_ip']))


#過往action ratio 比例變化是否巨大
record_sum = this_weekdays.groupby(['ds'],as_index=False)['record_count'].sum()

clientrst = this_weekdays[this_weekdays['action'] == 'client-rst']
clientrst_sum = clientrst.groupby(['ds'],as_index=False)['record_count'].sum()

serverrst = this_weekdays[this_weekdays['action'] == 'server-rst']
serverrst_sum = serverrst.groupby(['ds'],as_index=False)['record_count'].sum()

deny = this_weekdays[this_weekdays['action'] == 'deny']
deny_sum = deny.groupby(['ds'],as_index=False)['record_count'].sum()


merg1 = pd.merge(record_sum,clientrst_sum,on=['ds'],how='outer').fillna(0).rename(columns = {'record_count_x':'record_sum','record_count_y':'record_sum_client'})
merg2 = pd.merge(merg1,serverrst_sum,on=['ds'],how='outer').fillna(0).rename(columns = {'record_count':'record_sum_server'})
merg3 = pd.merge(merg2,deny_sum,on=['ds'],how='outer').fillna(0).rename(columns = {'record_count':'record_sum_deny'})

merg3['client_ratio'] = (merg3['record_sum_client'] / merg3['record_sum']) * 100
merg3['server_ratio'] = (merg3['record_sum_server'] / merg3['record_sum'])*100
merg3['deny_ratio'] = (merg3['record_sum_deny'] / merg3['record_sum'])*100


this_weekdays_filter_abnor = merg3[merg3['ds'] != abnormal_recent['ds'].values[0]]
abnormal_date = merg3[merg3['ds'] == abnormal_recent['ds'].values[0]]
client_percent_diff = abs(abnormal_date['client_ratio'].values[0] - np.mean(this_weekdays_filter_abnor['client_ratio']))
server_percent_diff = abs(abnormal_date['server_ratio'].values[0] - np.mean(this_weekdays_filter_abnor['server_ratio']))
deny_percent_diff = abs(abnormal_date['deny_ratio'].values[0] - np.mean(this_weekdays_filter_abnor['deny_ratio']))


if client_percent_diff > np.std(this_weekdays_filter_abnor['client_ratio']):
    abnormal_date['client_ratio_have_big_change'] = True
else:
    abnormal_date['client_ratio_have_big_change'] = False

if server_percent_diff > np.std(this_weekdays_filter_abnor['server_ratio']):
    abnormal_date['server_ratio_have_big_change'] = True
else:
    abnormal_date['server_ratio_have_big_change'] = False
    
if deny_percent_diff > np.std(this_weekdays_filter_abnor['deny_ratio']):
    abnormal_date['deny_ratio_have_big_change'] = True
else:
    abnormal_date['deny_ratio_have_big_change'] = False
    
#主要流量
sip_sum_sort = this_weekdays.groupby(['ds','dest_ip'],as_index=False)['total_sent_bytes'].sum().sort_values(by = ['ds','total_sent_bytes'],ascending=False).rename(columns = {'total_sent_bytes':'ds_dest_tatal_bytes'})
ds_sum = this_weekdays.groupby(['ds'],as_index=False)['total_sent_bytes'].sum().rename(columns = {'total_sent_bytes':'ds_tatal_bytes'})
sip_sum_sort1 = pd.merge(sip_sum_sort,ds_sum,on=['ds'],how='outer')
sip_sum_sort1['dest_ratio'] = sip_sum_sort1['ds_dest_tatal_bytes'] /sip_sum_sort1['ds_tatal_bytes']
sip_sum_sort1 = sip_sum_sort1.sort_values(by = ['ds','dest_ratio'],ascending=False)

aaaa = sip_sum_sort1.groupby(['dest_ip'],as_index=False)['dest_ratio'].median()
aaaa = aaaa.sort_values(by = ['dest_ratio'],ascending=False)
first_5 = aaaa.iloc[0:5]
other_dest = aaaa.iloc[4::]
draw_bar = pd.concat([first_5,pd.DataFrame({'dest_ip': ['other'], 'dest_ratio': [sum(other_dest['dest_ratio'])]})])
plt.bar(draw_bar['dest_ip'] , draw_bar['dest_ratio'])
plt.xticks(rotation = 70,fontsize = 12)
plt.yticks(fontsize = 12)
plt.ylabel('median of every week ratio',fontsize = 20)
plt.xlabel('session dest ip',fontsize = 20)

past_main_bytes_destip = sip_sum_sort1[(sip_sum_sort1['dest_ratio'] > 0.2) & (sip_sum_sort1['ds'] < abnormal_recent['ds'].values[0])]
past_main_bytes_destip['dest_ip'].unique()
abnormal_date_main_bytes_destip = sip_sum_sort1[(sip_sum_sort1['dest_ratio'] > 0.2) & (sip_sum_sort1['ds'] == abnormal_recent['ds'].values[0])]

if len(set(abnormal_date_main_bytes_destip['dest_ip']) & set(past_main_bytes_destip['dest_ip'])) < len(set(past_main_bytes_destip['dest_ip'])):
    abnormal_date['main_bytes_destip_change'] = True

#列出過往平均前5名的ip
sip_dest_mean = this_weekdays.groupby(['dest_ip'],as_index=False)['total_sent_bytes'].median().rename(columns = {'total_sent_bytes':'sum_total_sent_bytes'}).sort_values(by = ['sum_total_sent_bytes'],ascending=False)
sip_dest_mean.head(5)

plt.figure(figsize=(20,8))
ds_list = sorted(sip_sum_sort1['ds'].unique().tolist())
plt.plot(ds_list , np.ones(len(ds_list))*np.nan)
for key in sip_dest_mean.head(5)['dest_ip'].values:
#    key = '10.72.60.104'
    data = sip_sum_sort1.groupby(['dest_ip']).get_group(key)
    data = data.sort_values(by = ['ds'])
    plt.plot(data['ds'].tolist() , data['dest_ratio'],label = key,marker = 'o')
    plt.xticks(rotation=70)
plt.xlabel('every sunday',fontsize = 20)
plt.ylabel('Percentage of total_sent_bytes',fontsize = 20) 
plt.xticks(rotation=70,fontsize = 16) 
plt.yticks(rotation=70,fontsize = 16)     
plt.legend()
#=======================================================================================================#
raw_data_path = 'J:/Data_Anomaly_Detection/raw/fwip_serverfarm_source_dest/'

#raw_data讀取
for date in abnormal_recent['ds'].unique():
    date = '2020-12-27'
    int_date = eval(date.split('-')[0] + date.split('-')[1] + date.split('-')[2])
    
    file_list = os.listdir(raw_data_path)
    file_list1 = list(map(lambda x: x.split('_')[1],file_list))
    file_list1 = sorted(file_list1)
    file_list2 = list(filter(lambda x: (x >= (str(int_date)+'.csv')) & (x <= (str(int_date)+'.csv')) ,file_list1))
    ko = list(filter(lambda x: (x.split('_')[1]) in (list(set(file_list2))),file_list))
    ko = sorted(ko)
    
    all_df = []
    for file in tqdm(ko):
        try:
            df = pd.read_csv(raw_data_path + file , low_memory = True)
            all_df.append(df)
        except:
            pass
    all_df_Df = pd.concat(all_df)
    
    this_date_abnormal = abnormal_recent[abnormal_recent['ds'] == date]

    for sip in this_date_abnormal.groupby(['source_ip']):
        sip = '10.5.0.150'
        sip_data = this_date_abnormal.groupby(['source_ip']).get_group(sip)
        sip_data
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        