#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:47:42 2017

@author: yajun
"""

import pandas as pd

# read csv file directly from a URL and save the results
data = pd.read_csv('http://www-bcf.usc.edu/~gareth/ISL/Advertising.csv', index_col=0)

# display the first 5 rows
data.head()
'''
特征：
      TV  Radio  Newspaper  Sales
1  230.1   37.8       69.2   22.1
2   44.5   39.3       45.1   10.4
3   17.2   45.9       69.3    9.3
4  151.5   41.3       58.5   18.5
5  180.8   10.8       58.4   12.9

TV：对于一个给定市场中单一产品，用于电视上的广告费用（以千为单位）
Radio：在广播媒体上投资的广告费用
Newspaper：用于报纸媒体的广告费用
响应：
Sales：对应产品的销量
'''

import seaborn as sns

#%matplotlib inline
# visualize the relationship between the features and the response using scatterplots
sns.pairplot(data, x_vars=['TV','Radio','Newspaper'], y_vars='Sales', size=7, aspect=0.8)
'''
seaborn的pairplot函数绘制X的每一维度和对应Y的散点图。通过设置size和aspect参数来调节显示的大小和比例。
可以从图中看出，TV特征和销量是有比较强的线性关系的，而Radio和Sales线性关系弱一些，Newspaper和Sales线性关系更弱。
通过加入一个参数kind='reg'，seaborn可以添加一条最佳拟合直线和95%的置信带。
'''
sns.pairplot(data, x_vars=['TV','Radio','Newspaper'], y_vars='Sales', size=7, aspect=0.8, kind='reg')



# create a python list of feature names
feature_cols = ['TV', 'Radio', 'Newspaper']

# use the list to select a subset of the original DataFrame
X = data[feature_cols]

# equivalent command to do this in one line
X = data[['TV', 'Radio', 'Newspaper']]
# select a Series from the DataFrame
y = data['Sales']

# equivalent command that works if there are no spaces in the column name
y = data.Sales

from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

# default split is 75% for training and 25% for testing
print(X_train.shape)
print(y_train.shape)
print(X_test.shape)
print(y_test.shape)

from sklearn.linear_model import LinearRegression
linreg = LinearRegression()
linreg.fit(X_train, y_train)

print(linreg.intercept_, linreg.coef_)
# pair the feature names with the coefficients
print(feature_cols, linreg.coef_)
zip(feature_cols, linreg.coef_)

from sklearn import metrics
import numpy as np
y_pred = linreg.predict(X_test)
print(np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
'''
(1)平均绝对误差(Mean Absolute Error, MAE)
1n∑ni=1|yi−yi^|
(2)均方误差(Mean Squared Error, MSE)
1n∑ni=1(yi−yi^)2
(3)均方根误差(Root Mean Squared Error, RMSE)
1n∑ni=√1(yi−yi^)2−−−−−−−−−−−−−
'''
# define true and predicted response values
true = [100, 50, 30, 20]
pred = [90, 50, 50, 30]


# calculate MAE by hand
print("MAE by hand:",(10 + 0 + 20 + 10)/4.)
# calculate MAE using scikit-learn
print("MAE:",metrics.mean_absolute_error(true, pred))
# calculate MSE by hand
print("MSE by hand:",(10**2 + 0**2 + 20**2 + 10**2)/4.)
# calculate MSE using scikit-learn
print("MSE:",metrics.mean_squared_error(true, pred))
# calculate RMSE by hand
print("RMSE by hand:",np.sqrt((10**2 + 0**2 + 20**2 + 10**2)/4.))
# calculate RMSE using scikit-learn
print("RMSE:",np.sqrt(metrics.mean_squared_error(true, pred)))

'''
特征选择
在之前展示的数据中，我们看到Newspaper和销量之间的线性关系比较弱，现在我们移除这个特征，看看线性回归预测的结果的RMSE如何？
'''
feature_cols = ['TV', 'Radio']

X = data[feature_cols]
y = data.Sales

X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

linreg.fit(X_train, y_train)

y_pred = linreg.predict(X_test)

print(np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
