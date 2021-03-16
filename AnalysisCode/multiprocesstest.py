# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 14:41:29 2020

@author: Tony_Tien
"""

import concurrent.futures
import time
from multiprocessing import Process, Pool, cpu_count, set_start_method, Queue
number_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

def evaluate_item(x):
        # 计算总和，这里只是为了消耗时间
        result_item = count(x)
        # 打印输入和输出结果
        return result_item

def count(number) :
        for i in range(0, 10000000):
                i=i+1
        return i * number

if __name__ == "__main__":
        cpus = cpu_count()
        # 顺序执行
        start_time = time.time()
        all_result = []
        for item in number_list:
                all_result.append(evaluate_item(item))
        print(all_result)
        print("Sequential execution in " + str(time.time() - start_time), "seconds")
        # 线程池执行
        start_time_1 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(evaluate_item, item) for item in number_list]
                all_result = []
                for future in concurrent.futures.as_completed(futures):
                        all_result.append(future.result())
        print(all_result)
        print ("Thread pool execution in " + str(time.time() - start_time_1), "seconds")
        # 进程池
        start_time_2 = time.time()

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(evaluate_item, item) for item in number_list]
                all_result = []
                for future in concurrent.futures.as_completed(futures):
                        all_result.append(future.result())
        print(all_result)
        print ("Process pool execution in " + str(time.time() - start_time_2), "seconds")
        
