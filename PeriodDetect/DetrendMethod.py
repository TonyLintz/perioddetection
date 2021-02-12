# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 10:09:15 2020

@author: Tony_Tien
"""
import pywt
import numpy as np
from config import cutoff_frequency

def detrend_with_wavelet(a):
    w = pywt.Wavelet('sym5')
    mode = pywt.Modes.smooth
#    a = value['unique_dest_cnt']

    ca = []#近似分量
    cd = []#細節分量
    for i in range(7):
        (a, d) = pywt.dwt(a, w, mode)#進行5階離散小波變換
        ca.append(a)
        cd.append(d)
    
    rec_a = []
    rec_d = []
    
    for i, coeff in enumerate(ca):
        coeff_list = [coeff, None] + [None] * i
        rec_a.append(pywt.waverec(coeff_list, w))#重構
    
    for i, coeff in enumerate(cd):
        coeff_list = [None, coeff] + [None] * i
        if i ==3:
#            print(len(coeff))
#            print(len(coeff_list))
            pass
        rec_d.append(pywt.waverec(coeff_list, w))           
    return rec_a


def detrend_with_lowpass(array):
    fs = 1
    N = len(array)
    x_tick = (np.linspace(0,fs,len(array)))
    x_tick = list(map(lambda x: round(x,3) , x_tick))
    
    fftt = np.fft.fft(array)
    fftt[int(np.ceil(N*cutoff_frequency)): - (int(np.ceil(N*cutoff_frequency))-1)] = 0
    
    new_data = np.fft.ifft(fftt)
    ori_data = array
    Detrend = ori_data - new_data
    Detrend = list(map(lambda x: x.real,Detrend))
    return Detrend
