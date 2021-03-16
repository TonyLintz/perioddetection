# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 14:48:34 2021

@author: Tony_Tien
"""

import pandas as pd
import numpy as np
data_path = 'J:/Data_Anomaly_Detection/ap/for_bokeh/'
#data_path = 'J:/Data_Anomaly_Detection/dm/dws_infra_net_serverfarm_all_source_dest_port_agg_day/'
aaa = pd.read_csv(data_path + 'merge_pred_and_indicator_20210217.csv')
aaaa  = aaa[~aaa['period'].isna()]
aaaa = aaaa[(aaaa['ds'] >= '2020-11-01') & (aaaa['ds'] <= '2021-01-31')]
aaaaa = aaaa[((aaaa['udc_period_pr'] >= 80) & (aaaa['udc_period_pr'] < 90)) ]
aaaaa = aaaaa[['source_ip', 'ds', 'period', 'udc_period_pr', 'tsm_period_pr', 'unique_dest_cnt_sub', 'total_sent_mb_sub']]
