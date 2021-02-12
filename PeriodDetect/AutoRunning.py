# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 22:38:59 2020

@author: Tony_Tien
"""
import os
import shutil
from config import *
work_space = os.path.join(project_path , 'PeriodDetect\\')
SourceDataType_List = ['total_sent_bytes','unique_dest_cnt']
if_to_current = True
if_training = True
for SourceDataType in SourceDataType_List:
    f = os.popen(f"python {work_space}Detect_period_main.py --SourceData={SourceDataType} --if_to_current={if_to_current}")    
    print(f.read())
    f = os.popen(f"python {work_space}train_main.py --SourceData={SourceDataType} --if_to_current={if_to_current} --if_training={if_training}")    
    print(f.read())
    f =os.popen(f"python {work_space}Predict_main.py --SourceData={SourceDataType} --if_to_current={if_to_current}")    
    print(f.read())
  
    if SourceDataType == 'total_sent_bytes':
        SourceDataType_Small_Name = 'tsm'
    elif SourceDataType == 'unique_dest_cnt':
        SourceDataType_Small_Name = 'udc'
    
    shutil.copyfile(data_path + 'period_{}_result_file.csv'.format(SourceDataType_Small_Name), project_share_folder + 'Infra_Traffic_Log/ap/summary_peakNperiod/' + 'period_{}_result_file.csv'.format(SourceDataType_Small_Name) )
