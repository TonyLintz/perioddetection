'''
Created on 2019年5月16日

@author: ShihYao_Dai
'''
import csvReaderJobQueue
from readMultiCSV import readMultiCSV
from readMultiCSV import CSVAdapter
import csv
import logging
import datetime as dt
import os
import pandas as pd
import re
from Tools.scripts import byteyears
import queue
import threading
from tqdm import tqdm
from conf import conf


def main():   
    logger=logging.getLogger('ModuelMain') 
    
    csvAdapter=\
        CSVAdapter(
            sep='\t',
            func="",
            NumofThread=8,
            quoting=csv.QUOTE_NONE,
            usecols = None,
            DataEncode=conf.DataEncode,
            DataChunkSize=conf.DataChunkSize,
            DataSet=conf.DataSet
        )
        
    DF=csvAdapter.CollectData(conf.DataRoot)
    
    DF[conf.DataSet1].info()
    DF[conf.DataSet2].info()

def sayHello(tmpDF,filename):
    year,week=re.search("(201[0-9])_w([0-9]{1,2}).csv",filename).groups()
    year=int(year)
    week=int(week)
    tmpDF['PredictWeek']=week
    tmpDF['PredictYear']=year
    return tmpDF

if __name__ == '__main__':
    main()