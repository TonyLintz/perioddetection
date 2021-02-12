# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 11:52:48 2020

@author: Tony_Tien
"""

from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.base import clone
import numpy as np

#def Period_modeltraining(Feature_df):
##    All_amp = Feature_df['FFT_Amp']
#    train = Feature_df.loc[Feature_df.type == 'Training','FFT_Amp'].values
#    test = Feature_df.loc[Feature_df.type == 'Testing','FFT_Amp'].values
#    clf = OneClassSVM(nu=0.4, kernel="rbf", gamma=0.1)
##    normalize = preprocessing.scale(np.array(All_amp))
#
#    train = np.reshape(train , (len(train),1))
#    test = np.reshape(test , (len(test),1))
#    
#    scale = preprocessing.StandardScaler().fit(train)
#    normalize_train = scale.transform(train)
#    normalize_test = scale.transform(test)
#    
#    Train_and_test = np.reshape(np.append(normalize_train , normalize_test) , (len(np.append(normalize_train , normalize_test)),1))
##    Train_and_test = np.reshape(np.append(train , test) , (len(np.append(train , test)),1))
#    clf.fit(normalize_train)
#    predict = clf.predict(Train_and_test)
#    Feature_df['if_abnormal'] = predict
#    return Feature_df,Train_and_test


def Period_modeltraining(Train,source_ip):
    ocsvm = OneClassSVM(nu=0.4, kernel="rbf", gamma=0.1)
    clf = make_pipeline(StandardScaler(), ocsvm)
    Train = np.reshape(Train , (len(Train),1))
    model = clone(clf).fit(Train)
    model_info = {f"{source_ip}": model}
    return model_info

def Modeling_predict(Test,model):
    Test = np.reshape(Test , (len(Test),1))
    predict = model.predict(Test)
    return predict

