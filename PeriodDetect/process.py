import ipaddress
import numpy as np
import pandas as pd
import pywt
from tqdm import tqdm
from scipy.signal import find_peaks
import argparse
from PeriodDetect.Decorator import timing


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def remove_specify_path(top):
        
#    top = 'C:/Users/tony_tien/Desktop/git_project/peakdetection/PeriodChangeDetection/normal_and_abnormal_weekday_plot/'
    import os
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))


def creat_multi_path(path):
    import os
    import shutil
    if os.path.isdir(path):
        remove_specify_path(path)
        shutil.rmtree(path)
        os.makedirs(path)
    else:
        os.makedirs(path)
        
        
def create_is_server_column(data):
    import config
    ServerFarmIPList = []
    SIPList = data["source_ip"].unique()
    for OneSIP in SIPList:
        for s in config.ServerFarm:
            if (ipaddress.ip_address(OneSIP) in s):
                ServerFarmIPList.append(OneSIP)
    data["is_server"] = data["source_ip"].isin(ServerFarmIPList)
    return data


def fill_empty_ds(s): 
#  s = unique_dest_cnt1.groupby(['source_ip']).get_group('10.3.0.73')  
  min_date = min(s['ds'])
  max_date = max(s['ds'])
  idx = pd.date_range(min_date, max_date)
  
  s.index = pd.DatetimeIndex(s['ds'])
  s = s.reindex(idx, fill_value=0)
  s['ds'] = s.index
  s = s.reset_index(drop=True)
  s['source_ip'] = s['source_ip'].iloc[0]
  s['ds'] = s['ds'].astype(str)
  return s


def process_std(array):
    if len(set(array)) == 1:
        std = 0.1
    else:
        std = array.std()
    return std

    
def OnePeriodFill(test_data_raw, set_weekdays, mode, weekend_median, SourceDataType, all_fill_0_weekdays):

    test_data_raw_filter_na = test_data_raw[test_data_raw['valid_data'] != 0] #不用補植的人
    
    if (len(test_data_raw_filter_na) != mode) & (len(test_data_raw_filter_na) != 0):
        weekend_median_filter = weekend_median[weekend_median['weekdays'].isin(test_data_raw_filter_na['weekdays'])]

        if (weekend_median_filter['median_of_cycle'].mean()) != 0:
            ratio = ( (test_data_raw_filter_na[SourceDataType].mean()) / (weekend_median_filter['median_of_cycle'].mean()) )        
        else:
            ratio = 1
            
        for weekday in set_weekdays:
            if weekday in all_fill_0_weekdays:
                test_data_raw.loc[(test_data_raw.valid_data == 0) & (test_data_raw.weekdays == weekday),SourceDataType] = 0
            else:
                test_data_raw.loc[(test_data_raw.valid_data == 0) & (test_data_raw.weekdays == weekday),SourceDataType] = \
                weekend_median.loc[weekend_median.weekdays == weekday,'median_of_cycle'].values[0] * ratio
    elif len(test_data_raw_filter_na) == 0:
        test_data_raw.loc[:,SourceDataType] = weekend_median.loc[:,'median_of_cycle'].values
    else:
        pass
    return test_data_raw



def fill_value_method(value, mode, weekend_median, SourceDataType, all_fill_0_weekdays):
    set_weekdays = set(value['weekdays'].values)
    All_period = []
    for i in range(int(len(value)/mode)):
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        test_data_raw = OnePeriodFill(test_data_raw, set_weekdays, mode, weekend_median, SourceDataType, all_fill_0_weekdays)
        All_period.append(test_data_raw)
    All_period_df = pd.concat(All_period).sort_values(by = 'ds')
    return All_period_df



def fill_value_job(value, mode, SourceDataType):
    value = value.reset_index(drop=True)
    value.loc[value.valid_data == 0,SourceDataType] = np.nan
    not_nan = value[~value[SourceDataType].isna()]
    
    fill_empty = value[value['valid_data'] == 0]
    all_fill_0_weekdays = fill_empty.groupby(['source_ip','weekdays']).size().reset_index().rename(columns = {0:'empty_period_num'})
    all_fill_0_weekdays['empty_period_ratio'] = all_fill_0_weekdays['empty_period_num'] / int(len(value)/mode)
    all_fill_0_weekdays = all_fill_0_weekdays[all_fill_0_weekdays['empty_period_ratio'] > 0.9]['weekdays'].tolist()
    
    weekend_median = not_nan.groupby(['weekdays'],as_index=False)[SourceDataType].median().rename(columns = {SourceDataType:'median_of_cycle'})
    weekend_median = pd.merge(value[['weekdays']].drop_duplicates() , weekend_median , on =['weekdays'],how='left').fillna(0)
    weekend_median.loc[weekend_median.weekdays.isin(all_fill_0_weekdays) , 'median_of_cycle'] = 0
    weekend_median = weekend_median.sort_values(by = ['weekdays'])
    
    All_period_df = fill_value_method(value,mode,weekend_median,SourceDataType,all_fill_0_weekdays)
    return All_period_df
