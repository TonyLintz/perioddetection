# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 10:32:47 2020

@author: Tony_Tien
"""
import sys
import os
from datetime import datetime
import platform
import argparse
import ipaddress

from time_fun import get_Current_Date
from process import str2bool

#====== 引數設定 ============================#

parser = argparse.ArgumentParser(prog = "period abnormal detection config",\
        usage = "python XXX.py --SourceData=total_sent_bytes --if_to_current=True --if_to_current=True...",\
        description = "how to set project argument",\
        epilog = "http://172.22.37.77:8055/anomaly_detection/perioddetection/blob/master/README.md")

parser.add_argument(
    "--SourceData",
    type=str,
    default="total_sent_bytes",
    choices=["total_sent_bytes", "unique_dest_cnt", "clientrst_ratio", "serverrst_ratio", "deny_ratio"],
    help="what you using source data",
)
parser.add_argument(
    "--if_to_current",
    type=str2bool,
    default=True,
    choices=[True,False],
    help="whether set end date is current date",
)
parser.add_argument(
    "--if_training",
    type=str2bool,
    default=False,
    choices=[True,False],
    help="whether re-training model",
)
parser.add_argument(
    "--if_move_file",
    type=str2bool,
    default=False,
    choices=[True,False],
    help="whether moving file to ShareStorage",
)
SourceDataType = parser.parse_args().SourceData
if_to_current = parser.parse_args().if_to_current
if_training = parser.parse_args().if_training
if_move_file = parser.parse_args().if_move_file
#======== Environmental parameter ==========#
    
if platform.system() == 'Linux':
    project_share_folder = '/All/Data_Anomaly_Detection/'
    data_folder_path = os.path.join(project_share_folder,'dm/dws_infra_net_serverfarm_all_source_dest_port_agg_day/')
else:
    project_share_folder = 'J:/All/Data_Anomaly_Detection/'
    data_folder_path = os.path.join(project_share_folder,'dm/dws_infra_net_serverfarm_all_source_dest_port_agg_day/')
    
project_path = os.path.dirname(os.path.dirname(__file__))
pythonpaths = [
    os.path.join(project_path,'MultiCsvFileReader/Code/'),
    os.path.join(project_path,'PeriodDetect/'),
]
for p in pythonpaths:
    sys.path.append(p)


#Target time
Current_year = datetime.now().year
DataSet_reg=r"(before2020)|([0-9]+)"

#FileName、 DataRange and Parameter 
core_max = 6
mode = 7
TrainIP_condition_length = 30
train_start_date = '2020-06-01'
train_cut_date = '2020-11-30'

if if_to_current == True:
    test_cut_date = get_Current_Date()
else:
    test_cut_date = '2020-12-31'
    
model_name = "Period____{}____serverfarm_models".format(SourceDataType)
feature_set_name = "Period____{}____serverfarm_feauturesets".format(SourceDataType)
basis_set_name = "Period____{}____serverfarm_basisset".format(SourceDataType)

#abbreviation list
if SourceDataType == 'total_sent_bytes':
    SourceDataType_Small_Name = 'tsm'
elif SourceDataType == 'unique_dest_cnt':
    SourceDataType_Small_Name = 'udc'
elif SourceDataType == 'clientrst_ratio':
    SourceDataType_Small_Name = 'cr'
elif SourceDataType == 'serverrst_ratio':
    SourceDataType_Small_Name = 'sr'
elif SourceDataType == 'deny_ratio':
    SourceDataType_Small_Name = 'dr'    
#whether_have_period parameter
toleration = 4
mode_ratio = 0.4
critical_modenum = 5

#whether change period parameter
corr_peak_threshold = {'unique_dest_cnt':0.5,'total_sent_bytes':0.6,'clientrst_ratio':0.5,'serverrst_ratio':0.5,'deny_ratio':0.5}

#Detrend parameter
cutoff_frequency = 0.03

#using path
data_path = os.path.join(project_path,'data/')
log_path = os.path.join(project_path,'log/')
pickle_path = os.path.join(project_path,'pickle/')
major_plot_path = os.path.join(project_path,'{}_plot/'.format(SourceDataType))
sub_plot_path = os.path.join(major_plot_path,'normal_and_abnormal_weekday_plot/')
feature_scatter_path = os.path.join(major_plot_path,'feature_scatter/')
cycle_general_path = os.path.join(major_plot_path,'cycle_general/')
work_date_path = os.path.join(project_share_folder,'raw/dim_sc_day_ext/') 
share_model_path = os.path.join(project_share_folder,'model/PeriodDetection/') 
share_result_path = os.path.join(project_share_folder,'ap/summary/')

#Server farm IP
ServerFarmCIDR=[
    '10.3.0.0/24'
    ,'10.5.0.0/24'
    ,'10.4.0.0/24'
    ,'10.5.2.0/24'
    ,'10.5.10.0/24'
    ,'10.5.11.0/24'
    ,'10.5.9.0/24'
    ,'10.5.13.0/24'
    ,'10.5.12.0/24'
    ,'10.5.16.0/24'
    ,'10.5.20.0/24'
    ,'10.6.0.0/24'
    ,'10.64.57.0/24'
    ,'10.77.199.0/24'
    ,'10.77.248.0/24'
    ,'10.77.200.0/24'
    ,'10.77.201.0/24'
    ,'10.79.22.0/24'
    ,'10.79.32.0/24'
    ,'10.79.41.0/24'
    ,'10.200.4.0/24'
    ,'172.16.20.0/23'
    ,'172.16.56.0/24'
    ,'172.16.88.0/23'
    ,'172.16.90.0/24'
    ,'172.16.98.0/23'
    ,'172.17.0.0/16'
    ,'172.18.120.0/21'
    ,'172.18.128.0/17'
    ,'172.20.0.0/16'
    ,'172.21.10.0/23'
    ,'172.21.128.0/21'
    ,'172.21.136.0/22'
    ,'172.21.140.0/23'
    ,'172.21.252.0/24'
    ,'172.21.5.0/24'
    ,'172.21.6.0/23'
    ,'172.22.0.0/16'
    ,'172.23.0.0/16'
    ,'172.24.248.0/21'
    ,'172.26.0.0/16'
    ,'172.28.0.0/16'
    ,'172.29.0.0/16'
    ,'172.29.16.0/24'
    ,'172.29.8.0/24'
    ,'172.29.96.0/23'
    ,'172.30.0.0/23'
    ,'192.168.0.0/24'
    ,'192.168.100.0/24'
    ,'192.168.104.0/27'
    ,'192.168.108.0/27'
    ,'192.168.11.0/24'
    ,'192.168.121.0/24'
    ,'192.168.128.0/17'
    ,'192.168.14.0/23'
    ,'192.168.24.0/22'
    ,'192.168.28.0/24'
    ,'192.168.30.0/24'
    ,'192.168.42.0/24'
    ,'192.168.72.0/27'
    ,'192.168.75.0/24'
    ,'192.168.76.0/27'
    ,'192.168.77.0/24'
    ,'192.168.81.0/24'
    ,'192.168.82.0/24'
    ,'192.168.88.0/23'
    ,'192.168.142.0/23'
    ,'192.168.95.0/24'
    ,'192.168.188.0/23'
    ,'172.16.158.0/24'
]
ServerFarm=[]
for OneCIDR in ServerFarmCIDR:
    ServerFarm.append(ipaddress.ip_network(OneCIDR))
