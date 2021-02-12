'''
Created on 2019年5月16日

@author: ShihYao_Dai
'''

import threading
import logging
import pandas as pd
import datetime as dt
import csv
import csvReaderJobQueue
from tqdm import tqdm
import numpy as np
import mmap
import gc
import queue
import time


class readMultiCSV(threading.Thread):
    '''

    '''

    def __init__(
            self,
            ThdId,
            DataQueue,
            DataLock,
            JobQueue=csvReaderJobQueue.prepareCSVJob(),
            sep='\t',
            func="",
            quoting=csv.QUOTE_NONE,
            usecols = None,
            encoding='utf-8',
            ChunkSize=1000000
        ):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('readMultiCSV')
        self.setThdId=ThdId
        self.JobQueue=JobQueue
        self.DataQueue=DataQueue
        self.DataLock=DataLock
        self.final=pd.DataFrame({})
        self.sep=sep
        self.func=func
        self.quoting=quoting
        self.usecols=usecols
        self.encoding=encoding
        self.ChunkSize=ChunkSize
        
    def run(self):  
        while True:
            filename=self.JobQueue.qetJob(self.setThdId)
            FirstChunk=True
            ChunkList=[]
            if(len(filename)>0):
                with open(filename, 'r',encoding=self.encoding) as fileHandle:
                    buf = mmap.mmap(fileHandle.fileno(), 0,access=mmap.ACCESS_READ)
                    if(FirstChunk):
                        ChunkList=[buf.readline().replace(b'\n',b'').replace(b'\r',b'').decode().split(self.sep)]
                        FirstChunk=False
                                                
                    with tqdm(total=100, position=0, leave=True,desc='ThID {0} Reading...'.format(self.setThdId)) as pbar:
                        pbarValue=0
                        ChunkCount=0
                        orgPointer=0
                        while(buf.tell()<buf.size()):
                            try:
                                ChunkList+=[buf.readline().replace(b'\n',b'').replace(b'\r',b'').decode().split(self.sep)]
                                ChunkCount+=1
                                if (ChunkCount<self.ChunkSize):
                                    continue
                                else:
                                    ChunkCount=0
                            except StopIteration:
                                self.logger.debug("ThdID: %s had encountered StopInteration of file %s", self.setThdId, filename)
                                break
                            except ValueError:
                                self.logger.error("ThdID: %s had encountered ValueError of file %s", self.setThdId, filename)
                                break 
                            pbar.update(
                                int( 
                                    (buf.tell()-orgPointer)
                                    /buf.size()*100
                                )
                            )
                            pbarValue=\
                                pbarValue\
                                +int( 
                                    (buf.tell()-orgPointer)
                                    /buf.size()*100
                                )
                            orgPointer=buf.tell()
                                
                        if(pbarValue < 100):
                            pbar.update(100-pbarValue)
                   
                    if(callable(self.func)):
                        ChunkList=self.func(ChunkList,filename)
                    self.DataLock.acquire()
                    self.DataQueue.put(ChunkList)
                    self.DataLock.release()
                buf.close()
                gc.collect()

                self.logger.info("ThdID: %s had finished to load file %s", self.setThdId, filename)

            else:
                break


class CSVAdapter():
    
    def __init__(
            self,
            sep='\t',
            func="",
            NumofThread=16,
            quoting=csv.QUOTE_NONE,
            usecols = None,
            DataEncode=['utf-8'],
            DataChunkSize=[1000000],
            DataSet=[]
        ):
        '''
        Constructor
        '''
        self.logger = logging.getLogger('CSVAdapter')
        self.final=pd.DataFrame({})
        self.sep=sep
        self.func=func
        self.NumofThread=NumofThread
        self.quoting=quoting
        self.usecols=usecols
        self.DataEncode=DataEncode
        self.DataChunkSize=DataChunkSize
        self.DataQueue=queue.Queue()
        self.DataLock=threading.Lock()
        self.DataSet=DataSet
    
    
    def CollectData(self,FilePath):
        self.logger.info("create threads to read csv data")
        DF={}
        for dataset,ChunkSize,Encoding in zip(self.DataSet,self.DataChunkSize,self.DataEncode):
            threads = []    
            
            self.logger.info("scan list of csv file and push into job queue")
            jobQueue=csvReaderJobQueue.prepareCSVJob()
            jobQueue.ScanFileList(FilePath, dataset)
            self.logger.info("scan finished")
            
            if(jobQueue.que.qsize()>0):
                STime=time.time()
                for i in range(self.NumofThread):
                    threads.append(readMultiCSV(i,self.DataQueue,self.DataLock,jobQueue,",",self.func,encoding=Encoding,ChunkSize=ChunkSize))
                    threads[i].start()
                
                DF[dataset]=pd.DataFrame() 
                for i in range(self.NumofThread):
                        threads[i].join()
                
                ChunkList=[]
                GetColumnsName=False
                try:       
                    for _ in tqdm(range(self.DataQueue.qsize()),"Merge data...",position=0,leave=True):
                        Chunk=self.DataQueue.get()
                        if (GetColumnsName==False):
                            ChunkName=Chunk[:1][0]
                            GetColumnsName=True
                        ChunkList+=Chunk[1:]
                    DF[dataset]=pd.DataFrame(ChunkList,columns=ChunkName)
                except ValueError as e:
                    self.logger.error("{0} has value Error: {1}".format(dataset,e.args))
            
                self.logger.info("Threads were finished to read {0} data".format(dataset))
                self.logger.debug("Load Train Data {0:.2f} sec.".format(time.time()-STime))
                gc.collect()
            else:
                self.logger.error("There is not file been pushed into job queue")
        return  DF

    
