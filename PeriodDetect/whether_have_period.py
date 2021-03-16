# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 15:24:59 2020

@author: Tony_Tien
"""

from scipy import stats
import numpy as np
import pandas as pd
from tqdm import tqdm

from calculate import autocorrelation,find_corr_peak
from DetrendMethod import detrend_with_lowpass
from config import toleration,mode_ratio,critical_modenum
from PeriodDetect.Decorator import timing


@timing()
def detect_have_period(TrainData1,SourceDataType):

    All_key = []
    All_mode = []
    All_std = []
    All_flag = []
    All_diff = []
    
    for key,data in tqdm(TrainData1.groupby(['source_ip'])):
        try:
                value = data.copy()
                #去趨勢
                Detrend = detrend_with_lowpass(value[SourceDataType])
                value.loc[:,SourceDataType] = Detrend
                result = autocorrelation(np.array(value[SourceDataType]),len(value)-2)   
                
                #na太多是由於資料數值單調，故歸為沒有周期
                nan_ratio = len(list(filter(lambda x: np.isnan(x),result))) / len(result)
                if nan_ratio >= 0.5:
                    All_key.append(key)
                    All_mode.append(0)
                    All_std.append(0)
                    All_flag.append(0)
                    All_diff.append(np.nan)
                    continue
                
                #抓相關函數的peak
                df_peakk = find_corr_peak(result)
                mode = stats.mode(np.diff(df_peakk['index']))[0][0]  
                STD = np.std(np.diff(df_peakk['index']))
                
                #是否有周期判斷
                N = len(value)
                detect_period = mode
                Need_have_peak_num = round(int(N / detect_period))
                mode_in_difflist = list(filter(lambda x: x == mode,np.diff(df_peakk['index'])))
    
                if (abs(Need_have_peak_num - len(df_peakk)) < toleration) & (df_peakk['peak'].median() > mode_ratio) & (len(mode_in_difflist) >= critical_modenum) :

                    flag = 1
                else:
                    flag = 0
                
                All_key.append(key)
                All_mode.append(mode)
                All_std.append(STD)
                All_flag.append(flag)
                All_diff.append(abs(Need_have_peak_num - len(df_peakk)))
       
        except:
                All_key.append(key)
                All_mode.append(0)
                All_std.append(0)
                All_flag.append(0)
                All_diff.append(np.nan)
            
    cycle_df = pd.DataFrame({'sip':All_key,'mode':All_mode,'std':All_std, 'If_Real':All_flag, 'Diff':All_diff})
    have_cycle = cycle_df[cycle_df['If_Real'] == 1]   
    return  have_cycle
