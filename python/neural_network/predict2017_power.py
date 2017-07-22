#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 10:31:19 2017

@author: yajun
"""

import pandas as pd
import numpy  as np
from pymongo import *
from numpy import *

industr_code = 'D' # 电力、热力、燃气及水生产和供应业
client = MongoClient("mongodb://127.0.0.1:27017/")
#client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
db = client.bonds

def default_sample():
    code = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':industr_code},{"rptDate" : "20151231"}]},{'_id':0,'code':1,'COMP_NAME':1}) #,'COMP_NAME':1
    manus = pd.DataFrame(list(code)) # 获取制造业的代码 

#    make traing set from 2016
    qv = db.default_2016.find({'rptDate':'20161231'},{'_id':0,'rptDate':0}) 
    ratios = pd.DataFrame(list(qv)) # 获取技术性违约样本，从 2015年的财报中选择
    
    qv = db.issuers_info.find({'df':1},{'_id':0,'code':1})
    real_df = pd.DataFrame(list(qv)) #获取真实违约样本的代码
    
    print('违约的industry：')
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
    
    defaults = (OCFTOINTEREST.union(OCFTOQUICKDEBT).union(OCFTOSHORTDEBT))
#    defaults = (OCFTOQUICKDEBT).union(OCFTOSHORTDEBT)
    df_code = np.array(list(defaults))
    df = np.ones(len(df_code))
    samples = pd.DataFrame([df_code,df], ['code','df'])
    samples = samples.T
    
    print('真实违约与技术性违约的交集')
    print((samples.merge(real_df_manufacture,on='code')))
    real_df_manufacture.pop('COMP_NAME')
#    samples = samples.append(real_df_manufacture,ignore_index=True)
    return samples[['code','df']],real_df_manufacture

default,real_df_manu = default_sample()
#query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20141231"}]},{'_id':0,'COMP_NAME':0,'LISTINGORNOT':0,'INDUSTRY_CSRC12':0,'INDUSTRY_CSRCCODE12':0,'NATURE':0,'rptDate':0,'PROVINCE':0})

#rptDates={"$or":[{"rptDate" : "20151231"},{"rptDate" : "20161231"}]}
rptDates={"$or":[{"rptDate" : "20161231"},{"rptDate" : "20151231"}]}
#first_defaults = db.default.aggregate([{'$group' : {'_id' : "$发行人", 'date' : {'$first':"$发生日期"}}}])
#first_defaults = pd.DataFrame(list(first_defaults))

query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':industr_code},{"rptDate" : "20141231"}]},{'_id':0,'code':1})
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

#构建LM神经网络模型
#def LM_NN(result):
from keras.models import Sequential #导入神经网络初始化函数
from keras.layers.core import Dense, Activation, Dropout #导入神经网络层函数、激活函数
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
net.add(Dense(input_dim = n-1, output_dim = 32)) #添加输入层（3节点）到隐藏层（10节点）的连接
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dropout(0.5)) # over fitting
net.add(Dense(input_dim = 32, output_dim = 16)) #添加隐藏层1
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dropout(0.5)) # over fitting
net.add(Dense(input_dim = 16, output_dim = 16)) #添加隐藏层2
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dropout(0.5)) # over fitting
net.add(Dense(input_dim = 16, output_dim = 16)) #添加隐藏层3
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 16, output_dim = 16)) #添加隐藏层4
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 16, output_dim = 16)) #添加隐藏层5
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 16, output_dim = 16)) #添加隐藏层6
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 16, output_dim = 16)) #添加隐藏层7
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 16, output_dim = 16)) #添加隐藏层8
net.add(Activation('relu')) #隐藏层使用relu激活函数
net.add(Dense(input_dim = 16, output_dim = 1)) #添加输出层
net.add(Activation('sigmoid')) #输出层使用sigmoid激活函数  softmax: posibility
net.compile(loss = 'binary_crossentropy', optimizer = 'adam', class_mode = "binary") #编译模型，使用adam方法求解
m,n = np.shape(train)
net.fit(train[:,:n-1], train[:,-1], nb_epoch=10000, batch_size=1) #训练模型，循环1000次
#net.save_weights(netfile) #保存模型
predict_result = net.predict_classes(test[:,:n-1]).reshape(len(test)) #预测结果变形
import cm_plot  #导入自行编写的混淆矩阵可视化函数
cm_plot.cm_plot(test[:,n-1], predict_result).show()
import roc_plot
roc_plot.roc_plot(n,net,test).show()

predict_result16 = net.predict_classes(report16s.as_matrix()).reshape(len(report16s))
d2016=pd.DataFrame([code16.values,predict_result16],['code','df']).T
#print(d2016[d2016['df']==1])
query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':industr_code},{"rptDate" : "20141231"}]},{'_id':0,'COMP_NAME':1,'code':1})
manus = pd.DataFrame(list(query))
manus = manus.merge(d2016,on='code')
print(manus[manus['df']==1]['COMP_NAME'])
manus.to_excel("manufacture2017.xlsx")
