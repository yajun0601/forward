#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:47:42 2017

@author: yajun
"""

import pandas as pd
import numpy as np
from sklearn import metrics
from sklearn.preprocessing import StandardScaler
from pymongo import MongoClient


MISSING_VALUE = 0
def test():
    # read csv file directly from a URL and save the results
    data = pd.read_csv('http://www-bcf.usc.edu/~gareth/ISL/Advertising.csv', index_col=0)
    
    # display the first 5 rows
    data.head()
    '''
    特征：
          TV  radio  newspaper  sales
    1  230.1   37.8       69.2   22.1
    2   44.5   39.3       45.1   10.4
    3   17.2   45.9       69.3    9.3
    4  151.5   41.3       58.5   18.5
    5  180.8   10.8       58.4   12.9
    
    TV：对于一个给定市场中单一产品，用于电视上的广告费用（以千为单位）
    radio：在广播媒体上投资的广告费用
    newspaper：用于报纸媒体的广告费用
    响应：
    sales：对应产品的销量
    '''
    
    import seaborn as sns
    
    #%matplotlib inline
    # visualize the relationship between the features and the response using scatterplots
    sns.pairplot(data, x_vars=['TV','radio','newspaper'], y_vars='sales', size=7, aspect=0.8)
    '''
    seaborn的pairplot函数绘制X的每一维度和对应Y的散点图。通过设置size和aspect参数来调节显示的大小和比例。
    可以从图中看出，TV特征和销量是有比较强的线性关系的，而radio和sales线性关系弱一些，newspaper和sales线性关系更弱。
    通过加入一个参数kind='reg'，seaborn可以添加一条最佳拟合直线和95%的置信带。
    '''
    sns.pairplot(data, x_vars=['TV','radio','newspaper'], y_vars='sales', size=7, aspect=0.8, kind='reg')
    
    sns.pairplot(data, x_vars=['TV','radio'], y_vars='newspaper', size=7, aspect=0.8, kind='reg')
    
    # create a python list of feature names
    feature_cols = ['TV', 'radio', 'newspaper']
    
    # use the list to select a subset of the original DataFrame
    X = data[feature_cols]
    
    # equivalent command to do this in one line
    X = data[['TV', 'radio', 'newspaper']]
    # select a Series from the DataFrame
    y = data['sales']
    
    # equivalent command that works if there are no spaces in the column name
    y = data.sales
    
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
    

    y_pred = linreg.predict(X_test)
    print('calculate RMSE using scikit-learn:',np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
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
    在之前展示的数据中，我们看到newspaper和销量之间的线性关系比较弱，现在我们移除这个特征，看看线性回归预测的结果的RMSE如何？
    '''
    feature_cols = ['TV', 'radio']
    X = data[feature_cols]
    y = data.sales
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
    linreg.fit(X_train, y_train)
    y_pred = linreg.predict(X_test)
    print(np.sqrt(metrics.mean_squared_error(y_test, y_pred)))


# ============== YTM sklearn LR modle
def get_sample():
    from pymongo import MongoClient
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.ytm_std_samples.find({},{'_id':0,'name':0})
    record = pd.DataFrame(list(query))
    record = record[['defendant','shixin', 'zhixing','std', 'df']]
    sample = record.copy()
    sample_df=sample[sample['df']==1]
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    client.close()
    return sample


def get_sample_standard():
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds

    query = db.ytm_std_samples.find({},{'_id':0})
    record = pd.DataFrame(list(query))
    
    client.close()
    delta = get_stmt_footnotes()
    record = record.merge(delta, how='left', on='name')
    record = record.dropna() # drop any contains NA
    
    record = record[['defendant','shixin', 'zhixing','std', 'df','TOT_CUR_LIAB',
       'TOT_LIAB']]
    sample = record.copy()
    sample_df=sample[sample['df']==1]
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample = sample.append(sample_df)
    sample.reset_index(inplace=True, drop=True)
    df = sample.pop('df')
    standard =  StandardScaler().fit_transform(sample.values)
    record_sd = pd.DataFrame(standard)
    record_sd['df'] = df
    return record_sd

def get_stmt_footnotes():
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.default_bond_footnotes.find({},{'_id':0,'name':0})
    record_default = pd.DataFrame(list(query))
#    record_default.dropna(axis=0,how='any',thresh=4,inplace=True)
    record_default.fillna(0,inplace=True)
    footnote = record_default[['STMNOTE_AR_1', 'STMNOTE_AR_2', 'STMNOTE_EOITEMS_24',
       'STMNOTE_LTBORROW_4505', 'STMNOTE_OTHERS_4504', 'STMNOTE_OTHERS_7636',
       'STMNOTE_OTHERS_7637', 'STMNOTE_OTHERS_7639', 'STMNOTE_STBORROW_4505',
       'TOT_CUR_LIAB', 'TOT_LIAB']]
#    footnote = record_default[['TOT_CUR_LIAB', 'TOT_LIAB']]
    l,w = record_default.shape
    delta = pd.DataFrame()
    for x in range(int(l/2)):
#        print(record.iloc[x+1]-record.iloc[x]/(record.iloc[x]+0.0001))
        delta_rate = (footnote.iloc[2*x]-footnote.iloc[2*x+1])/(footnote.iloc[2*x+1])
        delta_rate['name'] = record_default.iloc[2*x]['issuer']
        delta = delta.append(delta_rate,ignore_index=True)
        
#    delta.dropna(axis=0,how='any',thresh=4,inplace=True)    
    delta=delta[['TOT_CUR_LIAB', 'TOT_LIAB','name']]    
    delta.fillna(MISSING_VALUE,inplace=True)
    delta.drop_duplicates(inplace=True)
    
    query = db.matured_bond_footnotes.find({},{'_id':0})
    record_matured = pd.DataFrame(list(query))
    #删除已到期但是发生违约的债券发行主体
    record_matured =  record_matured[~record_matured['issuer'].isin(record_default['issuer'])]    
#    record_matured.dropna(axis=0,how='any',thresh=4,inplace=True)    
    record = record_matured[['STMNOTE_AR_1', 'STMNOTE_AR_2', 'STMNOTE_EOITEMS_24',
   'STMNOTE_LTBORROW_4505', 'STMNOTE_OTHERS_4504', 'STMNOTE_OTHERS_7636',
   'STMNOTE_OTHERS_7637', 'STMNOTE_OTHERS_7639', 'STMNOTE_STBORROW_4505',
   'TOT_CUR_LIAB', 'TOT_LIAB']]
    l,w = record_matured.shape
    delta_matured = pd.DataFrame()
    for x in range(int(l/2)):
#        print(record.iloc[x+1]-record.iloc[x]/(record.iloc[x]+0.0001))
        delta_rate = (record.iloc[2*x]-record.iloc[2*x+1])/(record.iloc[2*x+1])
        delta_rate['name'] = record_matured.iloc[2*x]['issuer']
        delta_matured = delta_matured.append(delta_rate,ignore_index=True)
        
#    delta_matured.dropna(axis=0,how='any',thresh=4,inplace=True)
    delta_matured = delta_matured[['TOT_CUR_LIAB', 'TOT_LIAB','name']] 
    delta_matured.fillna(MISSING_VALUE,inplace=True)
    delta_matured.drop_duplicates(inplace=True)
    
    client.close()
    
    footnotes_delta = delta.append(delta_matured)
    return footnotes_delta

def get_2018_footnotes():
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.maturity2018_footnotes.find({},{'_id':0,'name':0})
    record_default = pd.DataFrame(list(query))
#    record_default.dropna(axis=0,how='any',thresh=4,inplace=True)
    record_default.fillna(0,inplace=True)
    footnote = record_default[['STMNOTE_AR_1', 'STMNOTE_AR_2', 'STMNOTE_EOITEMS_24',
       'STMNOTE_LTBORROW_4505', 'STMNOTE_OTHERS_4504', 'STMNOTE_OTHERS_7636',
       'STMNOTE_OTHERS_7637', 'STMNOTE_OTHERS_7639', 'STMNOTE_STBORROW_4505',
       'TOT_CUR_LIAB', 'TOT_LIAB']]
#    footnote = record_default[['TOT_CUR_LIAB', 'TOT_LIAB']]
    l,w = record_default.shape
    delta = pd.DataFrame()
    for x in range(int(l/2)):
#        print(record.iloc[x+1]-record.iloc[x]/(record.iloc[x]+0.0001))
        delta_rate = (footnote.iloc[2*x]-footnote.iloc[2*x+1])/(footnote.iloc[2*x+1])
        delta_rate['name'] = record_default.iloc[2*x]['issuer']
        delta = delta.append(delta_rate,ignore_index=True)
        
#    delta.dropna(axis=0,how='any',thresh=4,inplace=True)    
    delta=delta[['TOT_CUR_LIAB', 'TOT_LIAB','name']]    
    delta.fillna(MISSING_VALUE, inplace=True)
    delta.drop_duplicates(inplace=True)
    
    client.close()
        
    return delta
    
#data = get_sample()
data = get_sample_standard()
data.columns=[0,1,2,3,4,5,'df']
#sns.pairplot(record_sd, x_vars=[0,1,2,3], y_vars='df', size=7, aspect=0.8, kind='reg')
#sns.pairplot(record_sd, vars=[0,1,2,3,'df'])

# create a python list of feature names
feature_cols = [0,1,2,3,4,5]

# use the list to select a subset of the original DataFrame
X = data[feature_cols]
# equivalent command to do this in one line
#X = data[[1,2,3,4]]
# select a Series from the DataFrame
y = data['df']


from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

# default split is 75% for training and 25% for testing
print(X_train.shape)
print(y_train.shape)
print(X_test.shape)
print(y_test.shape)

from sklearn.linear_model import LinearRegression
from sklearn.linear_model.logistic import LogisticRegression
#linreg = LinearRegression()
linreg = LogisticRegression()
linreg.fit(X_train, y_train)

print(linreg.intercept_, linreg.coef_)
# pair the feature names with the coefficients
print(feature_cols, linreg.coef_)
zip(feature_cols, linreg.coef_)
y_pred = linreg.predict(X_test)
print("MAE:",metrics.mean_absolute_error(y_test, y_pred))
print("MSE:",metrics.mean_squared_error(y_test, y_pred))
print('RMSE:',np.sqrt(metrics.mean_squared_error(y_test, y_pred)))

from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
#y_test = [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
#y_pred = [0, 1, 0, 0, 0, 0, 0, 1, 1, 1]

y_pred = linreg.predict(X_test)
confusion_matrix=confusion_matrix(y_test,y_pred)
print(confusion_matrix)
plt.matshow(confusion_matrix)
plt.title('confusion_matrix')
plt.colorbar()
plt.ylabel('real type')
plt.xlabel('predicted type')
plt.show()

def roc_plot(modle, test, target):
#    from sklearn.metrics import roc_curve, roc_auc_score #导入ROC曲线函数
    from sklearn.metrics import roc_curve,auc
    import matplotlib.pyplot as plt
    predict_result = modle.predict(test).reshape(len(test))
    false_positive_rate, recall, thresholds = roc_curve(target, predict_result, pos_label=1)
#    auc = roc_auc_score(target, predict_result, average="macro", sample_weight=None)
    roc_auc = auc(false_positive_rate,recall)
    plt.plot(false_positive_rate, recall, linewidth=2, label = "AUC=%f"%(roc_auc)) #作出ROC曲线
    plt.title('Receiver Operating Characteristic')
    plt.xlabel('Fall-out') #坐标轴标签
    plt.ylabel('Recall') #坐标轴标签
    plt.ylim(0,1.05) #边界范围
    plt.xlim(0,1.05) #边界范围
    plt.legend(loc=4) #图例
    return plt #显示作图结果
    
#roc_plot(n,net,X_test, y_test).show()
roc_plot(linreg,X_test, y_test).show()


def predict_result(model):
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.ytm_std_samples_2018.find({},{'_id':0})
    record = pd.DataFrame(list(query))
    
    query = db.maturitydate2018_private_enterprise_bonds.find({},{'_id':0,'ISSUER':1})
    private_bond = pd.DataFrame(list(query))
    private_bond.columns=['name']
    
    sample = record.merge(private_bond,on='name',how='inner')
    delta = get_2018_footnotes()
    sample = sample.merge(delta, how='inner',on='name')
    
    client.close()
    
    name = sample.pop('name')
    sample = sample[['defendant','shixin', 'zhixing','std','TOT_CUR_LIAB','TOT_LIAB']]
    
    
    
    standard =  StandardScaler().fit_transform(sample.values)
    record_sd = pd.DataFrame(standard)
    values = np.mat(record_sd)*np.mat(linreg.coef_.T)
    

    pred = model.predict(record_sd)
    vv = np.asarray(values.T)
    result = pd.DataFrame(data=[name.values,vv[0],pred]).T
    result.drop_duplicates(inplace=True)
    result.columns = ['name','values','df']    
    
    result.sort_values(by='values',inplace=True)
    result.reset_index(drop=True,inplace=True)
    result.to_excel("predict_2018_default_privatebond_debt.xlsx")

    print(result[result['df']==1])
    
    result_df = result[result['df']==1]
    out = result_df.merge(record, on = 'name')
    
    
    
#predict_result(linreg)    

if __name__ == "__main__":    
    model = linreg
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client.bonds
    
    query = db.ytm_std_samples_2018.find({},{'_id':0})
    record = pd.DataFrame(list(query))
    
    query = db.maturitydate2018_private_enterprise_bonds.find({},{'_id':0,'ISSUER':1})
    private_bond = pd.DataFrame(list(query))
    private_bond.columns=['name']
    
    sample = record.merge(private_bond,on='name',how='inner')
    delta = get_2018_footnotes()
    sample = sample.merge(delta, how='inner',on='name')
    
    client.close()
    
    name = sample.pop('name')
    sample = sample[['defendant','shixin', 'zhixing','std','TOT_CUR_LIAB','TOT_LIAB']]
    
    
    
    standard =  StandardScaler().fit_transform(sample.values)
    record_sd = pd.DataFrame(standard)
    values = np.mat(record_sd)*np.mat(linreg.coef_.T)
    

    pred = model.predict(record_sd)
    vv = np.asarray(values.T)
    result = pd.DataFrame(data=[name.values,vv[0],pred]).T
    result.drop_duplicates(inplace=True)
    result.columns = ['name','values','df']    
    
    result.sort_values(by='values',inplace=True)
    result.reset_index(drop=True,inplace=True)
    result.to_excel("predict_2018_default_privatebond_debt.xlsx")

    l = len(result)
    print(result[int(l*0.7):l])
    
    result_df = result[result['df']==1]
    out = result_df.merge(record, on = 'name')
    
    
    
    
    

