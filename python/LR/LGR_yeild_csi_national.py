#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:47:42 2017

@author: yajun
"""

import pandas as pd
import numpy as np
from sklearn import metrics
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing.data import QuantileTransformer
from sklearn.preprocessing import StandardScaler
from pymongo import MongoClient
import json
# ============== YTM sklearn LR modle
def get_sample():
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.yeild_csi_std_samples.find({"$or":[{'df':1},{'df':0}]},{'_id':0}) #,'name':1
    record = pd.DataFrame(list(query))
    record = record[['defendant','shixin', 'zhixing','std', 'national','df']]
    sample = record.copy()
    sample_df=sample[sample['df']==1]
    sample = sample.append(sample_df)
#    sample = sample.append(sample_df)
#    sample = sample.append(sample_df)
#    sample = sample.append(sample_df)
#    sample = sample.append(sample_df)
#    sample = sample.append(sample_df)

    client.close()
    return sample

def set_national_ornot():
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db_comp = client.company_data
    query = db_comp.all_ratio_data.find({"NATURE":{"$regex":"国有企业"}},{"_id":0,"COMP_NAME":1})
    national_df = pd.DataFrame(list(query))
    national_df.drop_duplicates(inplace=True)
    national_df.columns=['name']
    national_df['national'] = 1

    db_bond = client.bonds
    query = db_bond.ytmb_std_samples.find({},{'_id':0})
    samples = pd.DataFrame(list(query))
    
    nation_sample=samples.merge(national_df,on='name',how='left')
    nation_sample['national'] = nation_sample['national'].fillna(0)
    
    # fill na with mean
#    nation_sample[['defendant', 'shixin', 'std', 'zhixing']] = nation_sample[['defendant', 'shixin', 'std', 'zhixing']].fillna(nation_sample.mean())
    # fill na with 0
    nation_sample[['defendant', 'shixin', 'std', 'zhixing']] = nation_sample[['defendant', 'shixin', 'std', 'zhixing']].fillna(0)

    insert_record = json.loads(nation_sample.to_json(orient='records'))
    ret = db_bond.ytmb_std_nantional_samples.insert_many(insert_record)
    return nation_sample
    
    
    
def get_sample_standard():
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.yeild_csi_std_samples.find({"$or":[{'df':1},{'df':0}]},{'_id':0})
    record = pd.DataFrame(list(query))
    client.close()
    
    record = record[['defendant','shixin', 'zhixing','std', 'national','df']]
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
#    'Data after quantile transformation (gaussian pdf)',
#    standard = QuantileTransformer(output_distribution='normal').fit_transform(sample.values)
    record_sd = pd.DataFrame(standard)
    record_sd['df'] = df
    return record_sd

def get_sample_Quantile():
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.yeild_csi_std_samples.find({"$or":[{'df':1},{'df':0}]},{'_id':0})
    record = pd.DataFrame(list(query))
    client.close()
    
    record = record[['defendant','shixin', 'zhixing','std','national', 'df']]
    record.fillna(0,inplace=True)
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
#    'Data after quantile transformation (gaussian pdf)',
    standard = QuantileTransformer(output_distribution='normal').fit_transform(sample.values)
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
    record = record_default[['STMNOTE_AR_1', 'STMNOTE_AR_2', 'STMNOTE_EOITEMS_24',
       'STMNOTE_LTBORROW_4505', 'STMNOTE_OTHERS_4504', 'STMNOTE_OTHERS_7636',
       'STMNOTE_OTHERS_7637', 'STMNOTE_OTHERS_7639', 'STMNOTE_STBORROW_4505',
       'TOT_CUR_LIAB', 'TOT_LIAB']]
    l,w = record_default.shape
    delta = pd.DataFrame()
    for x in range(int(l/2)):
#        print(record.iloc[x+1]-record.iloc[x]/(record.iloc[x]+0.0001))
        delta_rate = (record.iloc[2*x]-record.iloc[2*x+1])/(record.iloc[2*x+1])
        delta_rate['issuer'] = record_default.iloc[2*x]['issuer']
        delta = delta.append(delta_rate,ignore_index=True)
        
    delta.dropna(axis=0,how='any',thresh=4,inplace=True)
    
    
    
    query = db.matured_bond_footnotes.find({},{'_id':0,'name':0})
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
        delta_rate['issuer'] = record_matured.iloc[2*x]['issuer']
        delta_matured = delta_matured.append(delta_rate,ignore_index=True)
        
    delta_matured.dropna(axis=0,how='any',thresh=4,inplace=True)
    
    client.close()
    
    


#data = get_sample()
#data = get_sample_standard()
data = get_sample_Quantile()
data.columns=[0,1,2,3,4,'df']
#sns.pairplot(record_sd, x_vars=[0,1,2,3], y_vars='df', size=7, aspect=0.8, kind='reg')
#sns.pairplot(record_sd, vars=[0,1,2,3,'df'])

# create a python list of feature names
feature_cols = [0,1,2,3,4]

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
    from sklearn.metrics import roc_curve, roc_auc_score #导入ROC曲线函数
    import matplotlib.pyplot as plt
    predict_result = modle.predict(test).reshape(len(test))
    fpr, tpr, thresholds = roc_curve(target, predict_result, pos_label=1)
    auc = roc_auc_score(target, predict_result, average="macro", sample_weight=None)
    plt.plot(fpr, tpr, linewidth=2, label = "AUC=%f"%(auc)) #作出ROC曲线
    plt.xlabel('False Positive Rate') #坐标轴标签
    plt.ylabel('True Positive Rate') #坐标轴标签
    plt.ylim(0,1.05) #边界范围
    plt.xlim(0,1.05) #边界范围
    plt.legend(loc=4) #图例
    return plt #显示作图结果
    
#roc_plot(n,net,X_test, y_test).show()
roc_plot(linreg,X_test, y_test).show()


def predict_result(modle):
    client = MongoClient("mongodb://127.0.0.1:27017/")
    #client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
    db = client.bonds
    
    query = db.yeild_csi_std_samples.find({"$nor":[{'df':1},{'df':0}]},{'_id':0})
    record = pd.DataFrame(list(query))
    
    query = db.maturitydate2018_private_enterprise_bonds.find({},{'_id':0,'ISSUER':1})
    private_bond = pd.DataFrame(list(query))
    private_bond.columns=['name']
    
    sample = record #.merge(private_bond,on='name',how='inner')
    client.close()
    
    name = sample.pop('name')
    sample = sample[['defendant','shixin', 'zhixing','std','national']]
#    standard =  StandardScaler().fit_transform(sample.values)
    standard = QuantileTransformer(output_distribution='normal').fit_transform(sample.values)
    record_sd = pd.DataFrame(standard)
    values = np.mat(record_sd)*np.mat(linreg.coef_.T)
    
    pred = modle.predict(record_sd)
    vv = np.asarray(values.T)
    result = pd.DataFrame(data=[name.values,vv[0],pred]).T
    result.drop_duplicates(inplace=True)
    result.columns = ['name','values','df']    
    
    result.sort_values(by='values',inplace=True)
    result.to_excel("predict_2018_default_bond.xlsx")

    print(result[result['df']==1])
    result_df = result[result['df']==1]

#    out = result_df.merge(record, on = 'name')
    return result
    
    
    
#
if __name__ == "__main__":    
    result = predict_result(linreg)   
    
    l = len(result)
    print(result[int(l*0.7):l])
    
    result[int(l*0.7):].to_excel('predicted_2018_default_last_30percent.xlsx')
    
    
    
    
    

