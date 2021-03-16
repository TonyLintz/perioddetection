# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 14:09:58 2020

@author: Tony_Tien
"""

SourceDataType = 'total_sent_bytes'
SourceDataType = 'DataToLog'


x = value[SourceDataType]
lags = len(value)

n = len(x)
x = np.array(x)
result = [ np.correlate(x[i:]-x[i:].mean(),x[:n-i]-x[:n-i].mean())[0] / (x[i:].std()*x[:n-i].std()*(n-i))  for i in range(1,lags+1)]


plt.plot(x[i:]-x[i:].mean())
plt.plot(x[:n-i]-x[:n-i].mean())
plt.xticks(np.arange(len(x[:n-i]-x[:n-i].mean())) , np.arange(0,len(x[:n-i]-x[:n-i].mean())))





New_value = source_data.copy()

creat_multi_path(f'C:/Users/tony_tien/Desktop/tmp_plot/{key}/')
for  i in range(int(len(New_value)/mode)):
  
    test_data_raw = New_value[0+i*mode : (i+1)*mode].sort_values(by = ['weekdays'])
    holiday_df = test_data_raw[test_data_raw['is_server'] == '0']
    plt.plot(test_data_raw['weekdays'],test_data_raw[SourceDataType],marker = 'o')
#    plt.scatter(holiday_df['weekdays'],holiday_df[SourceDataType],color= 'goldenrod',s=200,label='Non-weekend-holiday')
    plt.title('{}_th period'.format(i))
    plt.xlabel('weekday')
    plt.ylabel(SourceDataType)
    plt.savefig(f'C:/Users/tony_tien/Desktop/tmp_plot/{key}/' + '{}_th period.png'.format(i))
    plt.close()


period1 = test_data_raw
period5 = test_data_raw
period43 = test_data_raw

corr = (stats.pearsonr(period1[SourceDataType],period5[SourceDataType])[0])


#--------------------------------------週期訊號模擬---------------------------------------------------------#
import numpy as np
import matplotlib.pyplot as plt

N = 30             # the number of points
Fs = 1.          # the sampling rate
Ts = 1./Fs          # the sampling period
freqStep = Fs/N     # resolution of the frequency in frequency domain
f = 1*freqStep     # frequency of the sine wave; folded by integer freqStep
t = np.arange(N)*Ts    # x ticks in time domain, t = n*Ts
noise = np.random.normal(0, 1, N)

A = 1000000
y1 = A * np.sin(2*np.pi*f*t) + A
y11 = np.log10(y1 + 1)

plt.plot(np.arange(N) , y1)

A = 1000000
y2 = A * np.sin(2*np.pi*f*t) + (A)
#y2[2] = 300000
#y2[6] = 400000
y2[22] = 300000
y2[25] = 200000
#y2 = y2 + np.std(y2) *2 * noise 
y22 = np.log10(y2 +1)
  
plt.plot(np.arange(N) , y22)

first_segment
second_segment

corr = stats.pearsonr(y1,y2)

np.correlate(y1-y1.mean(),y2-y2.mean())[0]  / len(y1)
y1.std()*y2.std()










