#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 16:25:35 2017

@author: yajun
"""

import pandas as pd
import numpy as np
from sklearn.datasets import load_boston
from sklearn.linear_model import SGDRegressor
from sklearn.model_selection import cross_val_score
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

def transcoding(x):
    l1 = list(df[10])
    province = list(set(l1))
    n = len(province)
    mat = [[0 for j in range(n)] for i in range(n)]
    province_dict = {}
    for i in range(n):
        mat[i][i] = 1
        province_dict[str(province[i])] = mat[i]
    ret = []
    for i in range(len(l1)):
        key = str(l1[i])
        ret.append(province_dict[key])
    return pd.DataFrame(ret),province_dict
    
#hot_coding,province_dict = transcoding(df[10])

def loadDataSet(filename):
    sheet = 0
    df = pd.read_excel(filename,sheetname=[sheet], header=None, skiprows=1)[sheet]
    df = df.dropna(how='any',thresh=df.shape[1]/2) # drop those rows 
    df = df.fillna(0)
    df[2] = df[2]/100000000  # 区间成交量
    df = df.sort_values(2).reset_index()
#    zeros = df[df[0]==0]
#    df = df.drop(zeros.index,axis=0)
    df[2] = standard(df[2]) #区间成交量
    
    df[3] = standard(df[3]) #债券余额
    df[4] = standard(df[4]) #债券期限
    df[6] = standard(df[6]) #每年付息次数
    df[9] = df[9].apply(map_01)  # 是否含权债
    df[11] = df[11].apply(map_01) # 是否上市公司
    df[14] = standard(df[14]) #大股东持股比例
    print( df.groupby(15).size())
    df[15] = standard(df[15].apply(map_rate)) #发行时主体评级
    print( df.groupby(15).size())
    df[16] = standard(df[16].apply(map_sub_rate)) #发行时主体评级展望
    df[17] = standard(df[17]) #存量债券余额

    target = df[2]
    data = df[[3,4,6,9,11,14,15,16,17]]
    
    import seaborn as sns
    sns.pairplot(df, x_vars=[3,4,6,9,11,14,15,16,17], y_vars=2, size=5, aspect=0.8, kind='reg')
    return np.mat(data),np.mat(target).T



def regression(filename):
    from sklearn.cross_validation import train_test_split
    print(filename)
    X,y = loadDataSet(filename)
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
    from sklearn.linear_model import LinearRegression
    from sklearn import metrics
    linreg = LinearRegression()
    linreg.fit(X_train, y_train)
    
#    print(linreg.intercept_, linreg.coef_)
    # pair the feature names with the coefficients
    feature_cols = ['债券余额', '债券期限', '每年付息次数','是否含权债','是否上市公司','大股东持股比例','发行时主体评级','发行时主体评级展望','存量债券余额']
    #print(feature_cols, linreg.coef_)
    #zip(feature_cols, linreg.coef_)
    y_pred = linreg.predict(X_test)
    
    print("MAE:",metrics.mean_absolute_error(y_test, y_pred))
    print("MSE:",metrics.mean_squared_error(y_test, y_pred))
    print('RMSE:',np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
    scores = cross_val_score(linreg, X, y,cv=5)
#    print(filename)  
    print("Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() * 2))
    
    res = pd.DataFrame(linreg.coef_,columns=feature_cols,index=[filename])
    return (res)

files = ['201603.xlsx','201604.xlsx','201605.xlsx','债券成交量3月.xlsx','债券成交量4月.xlsx','债券成交量5月.xlsx','债券成交量6月.xlsx']
#files = ['债券成交量3月.xlsx','债券成交量4月.xlsx','债券成交量5月.xlsx','债券成交量6月.xlsx']

params = pd.DataFrame()
for filename in files:
    params = params.append(regression(filename))
print(params.T)

    

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









