import queue
import threading

import pandas as pd
from loguru import logger
from MultiCsvFileReader.Code import csvReaderJobQueue
from MultiCsvFileReader.Code import readMultiCSV
from tqdm import tqdm

from PeakDetection.utility.decorator import timing


def CollectData(file_path, file_format, NumofThread=3):

    logger.info("scan list of csv file and push into job queue")
    jobQueue = csvReaderJobQueue.prepareCSVJob()
    jobQueue.ScanFileList(file_path, file_format)
    logger.info("scan finished")

    threads = []
    DataQueue = queue.Queue()
    DataLock = threading.Lock()
    for i in range(NumofThread):
        threads.append(readMultiCSV(i, DataQueue, DataLock, jobQueue, ",", ""))
        threads[i].start()
    DF = pd.DataFrame()

    logger.info("create threads to read csv data")
    for i in range(NumofThread):
        threads[i].join()

    for _ in tqdm(
        range(DataQueue.qsize()), "Merge data...", position=0, leave=True
    ):
        DF = DF.append(DataQueue.get())

    logger.info("Threads were finished to read activation data")
    print("data collection finished.")
    return DF
