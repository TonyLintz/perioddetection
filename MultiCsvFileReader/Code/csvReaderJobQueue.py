'''
Created on 2019年5月16日

@author: ShihYao_Dai
'''

import os
import queue
import threading
import logging
import datetime as dt
import re


class prepareCSVJob():
    #===============================================================================
    #把所有的csv file name放到queue等待thread來處理
    #===============================================================================


    def __init__(self):
        '''
        Constructor
        '''
        self.que=queue.Queue()
        self.queLock = threading.Lock()
        self.logger = logging.getLogger('PrepareCSVJob')

    #===============================================================================
    # 掃描目標路徑下的所有檔案
    #===============================================================================
    def ScanFileList(self,Path,regexp):
        for dirPath, _, fileNames in os.walk(Path):
            for filename in fileNames:
                ProcessFileFlag=False
                file_loc = os.path.join(dirPath, filename)
                if(re.search(regexp,file_loc.replace('\\','/'))!=None):
                    ProcessFileFlag=True
                if (ProcessFileFlag):
                    self.que.put(file_loc)
                    self.logger.debug("{} put into Job queue".format(file_loc))
                    
    #===============================================================================
    #設定特定要讀取的檔案
    #===============================================================================
    def insertFileList(self,file_loc):
        self.que.put(file_loc)
    
    def qetJob(self,ThdId):
        st=dt.datetime.now()
        self.queLock.acquire()
        WaitJobQueue=dt.datetime.now()-st
        if self.que.qsize() >0 :
            filename=self.que.get()
        else:
            filename=''
        self.queLock.release()
        self.logger.debug("ThdID: %s spends %s sec. to acquire queue", ThdId, WaitJobQueue)
        return filename