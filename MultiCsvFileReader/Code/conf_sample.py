'''
Created on 2020年11月20日

@author: ShihYao_Dai
'''

import datetime as dt
import logging
import os

class conf:
    #路徑設定
    DataRoot="../Data/"
    
    #資料集設定，採用regular expression
    DataSet1='[0-9]{1,}\.csv$'
    DataSet2='[0-9]{1,}\.csv$'
    DataSet3='[0-9]{1,}\.csv$'
    
    
    DataSet=[DataSet1,DataSet2,DataSet3]
    DataChunkSize=[100000,100000,100000]
    DataEncode=['utf8','utf8','utf8']
    
    loglevel="DEBUG"
    logfile= dt.datetime.now().strftime(DataRoot+"log/MultiCsvFileReader-%Y-%m-%d.log")
    if not (os.path.exists(DataRoot+"log/")):
        os.makedirs(DataRoot+"log/")
    logging.basicConfig(level=loglevel,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filename = logfile,
                        filemode='a')
    
    # 定義 handler 輸出 sys.stderr
    console = logging.StreamHandler()
    console.setLevel(loglevel)
    console.setFormatter(logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s'))
    logging.getLogger('').addHandler(console)
    