#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 10:31:19 2017

@author: yajun
"""

import pandas as pd
import numpy  as np
import json
from pymongo import *
from numpy import *


client = MongoClient("mongodb://127.0.0.1:27017/")
#client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
db = client.bonds

def default_sample():
    code = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20151231"}]},{'_id':0,'code':1,'COMP_NAME':1}) #,'COMP_NAME':1
    manus = pd.DataFrame(list(code)) # 获取制造业的代码 
    
    qv = db.default_2016.find({'rptDate':'20151231'},{'_id':0,'rptDate':0}) 
    ratios = pd.DataFrame(list(qv)) # 获取技术性违约样本，从 2015年的财报中选择
    
    qv = db.issuers_info.find({'df':1},{'_id':0,'code':1})
    real_df = pd.DataFrame(list(qv)) #获取真实违约样本的代码
    
    print('违约的制造业：')
    print(manus.merge(real_df,on='code'))
    
    real_df_ratio = real_df.merge(ratios, on='code')
    real_df_ratio = real_df_ratio.dropna(axis=0,how='any',thresh=2)
    real_df_manufacture = real_df_ratio.merge(manus,on='code')
    real_df_manufacture['df'] = 1.0
    
    result = manus.merge(ratios,on='code')
    
    result = result.fillna(result.mean())  # 用均值填充
    result = result.dropna(axis=0, how='any',thresh=2)
    num = int(0.05*len(result)) # get the last 5% record
    #现金流利息保障倍数
    result = result.sort_values('OCFTOINTEREST',axis=0, ascending=1)
    OCFTOINTEREST = set(result['code'][:num])
    #现金到期债务比
    result = result.sort_values('OCFTOQUICKDEBT',axis=0, ascending=1)
    OCFTOQUICKDEBT = set(result['code'][:num])
    # 经营活动产生的现金流量净额/流动负债
    result = result.sort_values('OCFTOSHORTDEBT',axis=0, ascending=1)
    OCFTOSHORTDEBT = set(result['code'][:num])
    
#    defaults = (OCFTOINTEREST.union(OCFTOQUICKDEBT).union(OCFTOSHORTDEBT))
    defaults = (OCFTOQUICKDEBT).union(OCFTOSHORTDEBT)
    df_code = np.array(list(defaults))
    df = np.ones(len(df_code))
    samples = pd.DataFrame([df_code,df], ['code','df'])
    samples = samples.T
    
    print('真实违约与技术性违约的交集')
    print((samples.merge(real_df_manufacture,on='code')))
    real_df_manufacture.pop('COMP_NAME')
    samples = samples.append(real_df_manufacture,ignore_index=True)
    return samples[['code','df']],real_df_manufacture

default,real_df_manu = default_sample()
query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20141231"}]},{'_id':0,'COMP_NAME':0,'LISTINGORNOT':0,'INDUSTRY_CSRC12':0,'INDUSTRY_CSRCCODE12':0,'NATURE':0,'rptDate':0,'PROVINCE':0})

#rptDates={"$or":[{"rptDate" : "20151231"},{"rptDate" : "20161231"}]}
rptDates={"$or":[{"rptDate" : "20161231"},{"rptDate" : "20151231"}]}
#first_defaults = db.default.aggregate([{'$group' : {'_id' : "$发行人", 'date' : {'$first':"$发生日期"}}}])
#first_defaults = pd.DataFrame(list(first_defaults))

query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20141231"}]},{'_id':0,'code':1})
manufactures = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_balance.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
balance = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_cashflow.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
cashflow = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_profit.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
profit = pd.DataFrame(list(query)) #Convert the input to an array.

tmp = balance.merge(cashflow, on=['code','rptDate'])
financial_report = tmp.merge(profit, on=['code','rptDate'])  # finacial report of 2015

#financial_report = financial_report.merge(manufactures, on='code')    
                            
report = financial_report.dropna(axis = 1, how='any', thresh=500)                   
report = report.fillna(report.mean())

report2015 = report[report['rptDate'] == '20151231']
report2015.pop('rptDate')
report2016 = report[report['rptDate'] == '20161231']
report2016.pop('rptDate')

shape(report2015)
shape(report2016)
report2015 = (report2015.merge(manufactures,on='code'))
report2016 = (report2016.merge(manufactures,on='code'))
# merge default falg
report2015 = report2015.merge(default, how='left',on='code')
report2015 = report2015.fillna(0)

code = report2015.pop('code')
df = report2015.pop('df')

code16 = report2016.pop('code')
#df16 = report2016.pop('df')
from sklearn.preprocessing import StandardScaler
standard =  StandardScaler().fit_transform(report2015.values)
report15s = pd.DataFrame(standard)
report15s['df'] = df

standard16 =  StandardScaler().fit_transform(report2016.values)
report16s = pd.DataFrame(standard16)
         
default = report15s[report15s['df']==1.0]
normal = report15s[report15s['df']==0.0]#.sample(len(default)*2)

#def LogisticRegression(result):
data = default.merge(normal, how='outer') # trainning samples

# 归一化
from cm_plot import * #导入自行编写的混淆矩阵可视化函数
def comp_plot(n,predict_result):
    cm_plot(train[:,n-1], predict_result).show() #显示混淆矩阵可视化结果

def plot_roc(n,net, test):
    from sklearn.metrics import roc_curve #导入ROC曲线函数
    import matplotlib.pyplot as plt
    predict_result = net.predict(test[:,:-1]).reshape(len(test))
    fpr, tpr, thresholds = roc_curve(test[:,-1], predict_result, pos_label=1)
    plt.plot(fpr, tpr, linewidth=2, label = 'ROC of LM') #作出ROC曲线
    plt.xlabel('False Positive Rate') #坐标轴标签
    plt.ylabel('True Positive Rate') #坐标轴标签
    plt.ylim(0,1.05) #边界范围
    plt.xlim(0,1.05) #边界范围
    plt.legend(loc=4) #图例
    plt.show() #显示作图结果

#构建LM神经网络模型
#def LM_NN(result):
from keras.models import Sequential #导入神经网络初始化函数
from keras.layers.core import Dense, Activation #导入神经网络层函数、激活函数
#    dd = pd.DataFrame(Variance)
dd = default.merge(normal, how='outer')
dd=dd.sort_values(1,ascending=0)
nn_data = dd.as_matrix()
#shuffle(nn_data)
p = 0.8 # train/test ratio
m,n = shape(nn_data)
train = nn_data[:int(m*p),:]
test = nn_data[int(m*p):,:]
print(sum(train[:,-1]),sum(test[:,-1]))
netfile = './net2016.model' #构建的神经网络模型存储路径

net = Sequential() #建立神经网络
net.add(Dense(input_dim = n-1, output_dim = 50)) #添加输入层（3节点）到隐藏层（10节点）的连接
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 50, output_dim = 20)) #添加输入层（3节点）到隐藏层（10节点）的连接
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 20, output_dim = 1)) #添加隐藏层（10节点）到输出层（1节点）的连接
net.add(Activation('sigmoid')) #输出层使用sigmoid激活函数  softmax: posibility
net.compile(loss = 'binary_crossentropy', optimizer = 'adam', class_mode = "binary") #编译模型，使用adam方法求解
m,n = np.shape(train)
net.fit(train[:,:n-1], train[:,n-1], nb_epoch=100, batch_size=1) #训练模型，循环1000次
net.save_weights(netfile) #保存模型
predict_result = net.predict_classes(train[:,:n-1]).reshape(len(train)) #预测结果变形
comp_plot(n,predict_result)
plot_roc(n,net,test)

predict_result16 = net.predict_classes(report16s.as_matrix()).reshape(len(report16s))
d2016=pd.DataFrame([code16.values,predict_result16],['code','df']).T
print(d2016[d2016['df']==1])
query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20141231"}]},{'_id':0,'COMP_NAME':1,'code':1})
manus = pd.DataFrame(list(query))
manus = manus.merge(d2016,on='code')
manus.to_excel("manufacture2017.xlsx")
'''这里要提醒的是，keras用predict给出预测概率，predict_classes才是给出预测类别，而且两者的预测结果都是n x 1维数组，而不是通常的 1 x n'''

'''
reportDate = '20161231'

first_defaults = db.default.aggregate([{'$group' : {'_id' : "$发行人", 'date' : {'$first':"$发生日期"}}}])
first_defaults = pd.DataFrame(list(first_defaults))

query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20151231"}]},{'_id':0,'code':1}) #,'COMP_NAME':1
manus = pd.DataFrame(list(query)) # 获取制造业的代码 

                        
query = db.bond_balance.find({"rptDate" : reportDate},{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0, 'rptDate':0})
balance = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_cashflow.find({"rptDate" : reportDate},{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0,'rptDate':0})
cashflow = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_profit.find({"rptDate" : reportDate},{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0,'rptDate':0})
profit = pd.DataFrame(list(query)) #Convert the input to an array.

tmp = balance.merge(cashflow, on='code')
tmp = tmp.merge(manus, on='code')
financial_report = tmp.merge(profit, on='code')  # finacial report of 2015
                           
code = financial_report.pop('code')

report = financial_report.fillna(financial_report.mean())
report.dropna(axis=1,how='any',inplace=True)
from sklearn.preprocessing import StandardScaler
standard =  StandardScaler().fit_transform(report.values)
report = pd.DataFrame(standard)
nn_data = report.as_matrix()

#构建LM神经网络模型
#def LM_NN(result):
from keras.models import Sequential #导入神经网络初始化函数
from keras.layers.core import Dense, Activation #导入神经网络层函数、激活函数
netfile = './net.model' #构建的神经网络模型存储路径
net = Sequential() #建立神经网络
m,n = shape(nn_data)
net.add(Dense(input_dim = n-1, output_dim = 100)) #添加输入层（3节点）到隐藏层（10节点）的连接
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 100, output_dim = 20)) #添加输入层（3节点）到隐藏层（10节点）的连接
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 20, output_dim = 1)) #添加隐藏层（10节点）到输出层（1节点）的连接
net.add(Activation('sigmoid')) #输出层使用sigmoid激活函数  softmax: posibility
net.compile(loss = 'binary_crossentropy', optimizer = 'adam', class_mode = "binary") #编译模型，使用adam方法求解
net.load_weights(netfile) #: 加载保存在save_weights中模型权值. 只能加载相同结构的文件.
#predict_result = net.predict_classes(nn_data).reshape(nn_data) #预测结果变形
#comp_plot(n,predict_result)
#plot_roc(n,net,test)                 
'''              