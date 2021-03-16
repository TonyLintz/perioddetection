# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 17:50:21 2020

@author: Tony_Tien
"""

import sys
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import pickle
import concurrent.futures
from distutils.util import strtobool
from matplotlib import pyplot as plt
from sklearn.svm import OneClassSVM
from sklearn import  ensemble, preprocessing, metrics
from datetime import datetime
from matplotlib import pyplot as plt
from DetrendMethod import detrend_with_lowpass
from config import *
from calculate import *
from ModelTraining import *
from plot_function import *

SourceDataType = 'total_sent_bytes'
Have_cycle_source_data = pd.read_csv(data_path + '{}___Have_cycle_source_data(fillvalue).csv'.format(SourceDataType))
Have_cycle_source_data['Datasqrt'] = pow(Have_cycle_source_data['total_sent_bytes'], 1/5)
Have_cycle_source_data['Datalog'] = np.log10(1 + Have_cycle_source_data['total_sent_bytes'])

SourceDataType = 'Datasqrt' 
#SourceDataType = 'total_sent_bytes'
for key ,source_data in Have_cycle_source_data.groupby(['source_ip']):
        
        key = '172.21.129.23'
        source_data = Have_cycle_source_data.groupby(['source_ip']).get_group(key)
        source_data = source_data.reset_index(drop=True)
        weekend_holiday_part = source_data[source_data['holiday_type'] == 'weekend-holiday']
        Non_weekend_holiday_part = source_data[source_data['holiday_type'] == 'Non-weekend-holiday']

        plt.figure(figsize = (20,8))
        
        for x1,x2,y1,y2 in zip(range(len(source_data)) ,range(len(source_data))[1:] , source_data[SourceDataType].values  , source_data[SourceDataType].values[1:]):
            if source_data.loc[x2,'is_server'] == '0':
                p1 = plt.plot([x1, x2], [y1, y2], 'b')
            else:
                p2 = plt.plot([x1, x2], [y1, y2], 'b')     
         
        
        
#        plt.plot(source_data['ds'],source_data[SourceDataType],'b')
#        plt.scatter(weekend_holiday_part.index,weekend_holiday_part[SourceDataType],color = 'm',s=100,label = 'weekend-holiday')
#        plt.scatter(Non_weekend_holiday_part.index,Non_weekend_holiday_part[SourceDataType],color = 'goldenrod',s=100,label = 'Non-weekend-holiday')
        plt.title(key)
        plt.xlabel('ds')
        plt.ylabel(SourceDataType)
        plt.yticks(rotation = 90,fontsize=12)
        plt.legend()
        plt.savefig('C:/Users/tony_tien/Desktop/tmp_plot1/{}.png'.format(key))
        plt.close()
