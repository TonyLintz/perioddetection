# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 10:07:32 2020

@author: Tony_Tien
"""
from scipy.signal import find_peaks
from config import corr_peak_threshold,SourceDataType
import numpy as np
import pandas as pd
from process import process_std

def autocorrelation(x,lags):
#    x = value['unique_dest_cnt']
#    lags = len(value)
    
    n = len(x)
    x = np.array(x)
    result = [np.correlate(x[i:]-x[i:].mean(),x[:n-i]-x[:n-i].mean())[0]\
		/(x[i:].std()*x[:n-i].std()*(n-i)) \
		for i in range(1,lags+1)]
    return result


def find_corr_peak(result):
    All_index = [] 
    for i in range(4,len(result)-3):
       if (result[i] > result[i-1]) & (result[i] > result[i-2]) &\
           (result[i] > result[i-3]) & (result[i] > result[i-4]) &\
           (result[i] > result[i+1]) & (result[i] > result[i+2]) &\
           (result[i] > result[i+3]):
           All_index.append(i)
    peakk = np.array(result)[All_index]
    df_peak = pd.DataFrame({'index':All_index,'peak':peakk})
    df_peakk = df_peak[df_peak.peak > 0]
    return df_peakk


#def find_corr_peak(result):
#    peaks2, _ = find_peaks(result, prominence=0.2) 
#    peakk = np.array(result)[peaks2]
#    df_peak = pd.DataFrame({'index':peaks2,'peak':peakk})
#    df_peakk = df_peak[df_peak.peak > 0]
#    return df_peakk


def calculate_overall_feature(value,basis,mode):
    All_amp = []
    for  i in range(int(len(value)/mode)):
        
        test_data_raw = value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
        test_data = test_data_raw[SourceDataType].values
        
        corpeak_mean,Amp = calculate_correlation_with_trainbasis(test_data,basis,mode)
#        All_amp.append(Amp)
        All_amp.append(corpeak_mean)
        
    All_amp = np.array(All_amp)
    where_are_NaNs = np.isnan(All_amp)
    All_amp[where_are_NaNs] = 0
    return All_amp


def calculate_correlation_with_trainbasis(test_data,basis,mode):
        
        test_std = process_std(test_data)  
        All_cor = []
        for j in range(len(basis)):
            window_train = basis[j:j+mode].sort_values(by = ['weekdays'])[SourceDataType].values
            window_train_std = process_std(window_train)            
         
            if len(window_train) == mode:
                cor = np.correlate(test_data-test_data.mean(),window_train-window_train.mean())[0] / (test_std*window_train_std*mode)
                All_cor.append(cor)
            else:
                pass
            
#        aa =  find_corr_peak(All_cor)          
        Amp = calculate_fft_amp_with_correlation(All_cor,mode)
#        corpeak_mean = aa['peak'].mean()
        corpeak_mean = np.mean(All_cor)
        return corpeak_mean,Amp

 
def calculate_fft_amp_with_correlation(All_cor,mode):
    y_fft = np.fft.fft(All_cor) 
    index = abs(y_fft).tolist().index( max([ abs(y_fft)[int(np.floor(len(All_cor) / mode))] , abs(y_fft)[int(np.ceil(len(All_cor) / mode))] ])  ) 
    Amp = abs(y_fft)[index]
    return Amp


def transform_label(Feature_df):
    normal_mean = Feature_df[Feature_df['if_abnormal']  == 1]['FFT_Amp'].mean()
    Feature_df.loc[(Feature_df.FFT_Amp > normal_mean), 'if_abnormal'] = 1
    return Feature_df


def Resurrection(Feature_df):
    Feature_df.loc[(Feature_df.FFT_Amp > corr_peak_threshold[SourceDataType]), 'if_abnormal'] = 1
    return Feature_df


