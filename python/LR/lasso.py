#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 16:25:35 2017

@author: yajun
"""

import numpy as np
from sklearn.datasets import load_boston
from sklearn.linear_model import SGDRegressor
from sklearn.cross_validation import cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.cross_validation import train_test_split


data = load_boston()
X_train, X_test, y_train, y_test = train_test_split(data.data, data.target)
X_scaler = StandardScaler()
y_scaler = StandardScaler()
X_train = X_scaler.fit_transform(X_train)
y_train = y_scaler.fit_transform(y_train)#.reshape(-1, 1)
X_test = X_scaler.transform(X_test)
y_test = y_scaler.transform(y_test)#.reshape(-1, 1)
regressor = SGDRegressor(loss='squared_loss',penalty="l1")
scores = cross_val_score(regressor, X_train, y_train, cv=5)#.reshape(-1, 1)
print('cv R', scores)
print('mean of cv R', np.mean(scores))
regressor.fit_transform(X_train, y_train)
print('Test set R', regressor.score(X_test, y_test))

def map_01(x):
    if x == '是':
        ret = 1
    else:
        ret = 0
    return ret
def map_rate(x):
    rates=['AAA','AA+','AA','AA-','A+','A','A-','BBB+','BBB','BBB-','BB+','BB','BB-','B+','B','B-','CCC+','CCC','CCC-','CC+','CC','CC-','DDD']
    ret = 0
    for i in rates:
        if x == i:
            ret = len(rates) - rates.index(i)
    return ret

def map_sub_rate(x):
    sub_rate=['负面','稳定','正面']
    ret = 0
    if x == sub_rate[0]:
        ret = -1
    elif x == sub_rate[1]:
        ret = 0
    elif x == sub_rate[2]:
        ret = 1
    return ret

def standard(x):
    mean = x.mean()
    std = x.std()
    ret = (x-mean)/std
    return ret

def loadDataSet(filename):
    df = pd.read_excel(DATA_FILE,sheetname=[1], header=None, skiprows=1)[1]
    df = df.fillna(0)
    df[2] = df[2]/100000000  # 区间成交量
    zeros = df[df[0]==0]
#    df = df.drop(zeros.index,axis=0)
    df[2] = standard(df[2])
    df[3] = standard(df[3])
    df[4] = standard(df[4])
    df[6] = standard(df[6])
    df[15] = standard(df[14])
    df[17] = standard(df[17])


    df[9] = df[9].apply(map_01)  # 是否含权债
    df[11] = df[11].apply(map_01) # 是否上市公司
    df[15] = df[15].apply(map_rate) #发行时主体评级
    df[16] = df[16].apply(map_sub_rate) #发行时主体评级展望
       
    return df

import pandas as pd
DATA_FILE='SH_StockExchange.xlsx'
df = pd.read_excel(DATA_FILE,sheetname=[1], header=None, skiprows=1)[1]
df = df.fillna(0)
df[0] = df[0]/100000000
zeros = df[df[0]==0]
data = df.drop(zeros.index,axis=0)
#sns.pairplot(data, x_vars=[1,2,3,4,5], y_vars=0, size=7, aspect=0.8, kind='reg')
#sns.pairplot(data, vars=[0,1,2,3,4])

# create a python list of feature names
feature_cols = [1,2,3,4,5]

# use the list to select a subset of the original DataFrame
X = data[feature_cols]

# select a Series from the DataFrame
y = data[0] 

regressor = SGDRegressor(loss='squared_loss',penalty="l1")
scores = cross_val_score(regressor, X_train, y_train.reshape(-1, 1), cv=5)#.reshape(-1, 1)
print('cv R', scores)
print('mean of cv R', np.mean(scores))
regressor.fit_transform(X_train, y_train.reshape(-1, 1))
print('Test set R', regressor.score(X_test, y_test.reshape(-1, 1)))

for i in range(len(y_test)):
    y_h = regressor.predict(X_test[i].reshape(1,-1))
    print(y_test[i]-y_h, y_test[i],y_h)




from sklearn import datasets, linear_model
from sklearn.model_selection import cross_val_score
diabetes = datasets.load_diabetes()
X = diabetes.data[:150]
y = diabetes.target[:150]
lasso = linear_model.Lasso()
print(cross_val_score(lasso, X, y))  









