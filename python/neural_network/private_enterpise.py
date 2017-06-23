#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 11:33:08 2017

@author: yajun

1. read financial report of all issuers 
2. merge with private_enterpirse with COMP_NAME
3. make technical default samples
4. prepare training sets

"""
import pandas as pd
import numpy as np
import json
from pymongo import MongoClient

client = MongoClient("mongodb://192.168.10.60:27017/")
#client = pymongo.Connection(host='192.168.10.60',port=27017)
db = client.bonds
#DATA_FILE='../neural_network/民营企业-信用债.xlsx'
#issuer = pd.read_excel(DATA_FILE,sheetname='issuers', header = 0)

#excel = pd.read_excel(DATA_FILE,sheetname=0, header = 0, skip_footer = 2)
#issuer = set(excel['发行人中文名称'].values)
##insert into database
#insert_record = json.loads(excel.to_json(orient='records'))
#ret = db.private_enterprise.insert_many(insert_record)
            
# 确定这张表与已有财报的交集
#
def get_issuer():
    query = db.bond_balance.find({'rptDate':'20151231'},{'_id':0,'COMP_NAME':1, 'code':1})
    name_code = pd.DataFrame(list(query)) #Convert the input to an array.
    query = db.private_enterprise.find({},{'_id':0, 'code':0, '证券简称':0})
    issuer = pd.DataFrame(list(query)).drop_duplicates()
    issuer = issuer.merge(name_code, how='inner', on='COMP_NAME')
    return issuer

def get_report():
    rptDates={"$or":[{"rptDate" : "20161231"},{"rptDate" : "20151231"}]}
    
    query = db.bond_balance.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
    balance = pd.DataFrame(list(query)) #Convert the input to an array.
    
    query = db.bond_cashflow.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
    cashflow = pd.DataFrame(list(query)) #Convert the input to an array.
    
    query = db.bond_profit.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
    profit = pd.DataFrame(list(query)) #Convert the input to an array.
    
    tmp = balance.merge(cashflow, on=['code','rptDate'])
    financial_report = tmp.merge(profit, on=['code','rptDate'])  # finacial report of 2015
                            
    private_report = financial_report.merge(get_issuer(), on='code')
    
    report = private_report.dropna(axis = 1, how='any', thresh=int(len(private_report)*0.25))                   
    report = report.fillna(report.mean())
    
    report2015 = report[report['rptDate'] == '20151231']
    report2015 = report2015.drop(['rptDate','COMP_NAME'], axis=1)
    report2016 = report[report['rptDate'] == '20161231']
    report2016 = report2016.drop(['rptDate','COMP_NAME'], axis=1)
    return report2015,report2016

def default_sample():
    # generate technical defaults samples
    qv = db.default_2016.find({'rptDate':'20161231'},{'_id':0,'rptDate':0})
    ratios = pd.DataFrame(list(qv)) # 获取技术性违约样本，从 2015年的财报中选择
    
    issuer = get_issuer()
    result = issuer.merge(ratios,on='code')                     
        
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
    return samples

sample = default_sample()
report2015,report2016 = get_report()
report2015 = report2015.merge(sample, how='left',on='code')
report2015 = report2015.fillna(0)

code = report2015.pop('code')
df = report2015.pop('df')

code16 = report2016.pop('code')
#df16 = report2016.pop('df')


# 归一化
from sklearn.preprocessing import StandardScaler
standard =  StandardScaler().fit_transform(report2015.values)
report15s = pd.DataFrame(standard)
report15s['df'] = df

standard16 =  StandardScaler().fit_transform(report2016.values)
report16s = pd.DataFrame(standard16)
         
default = report15s[report15s['df']==1.0]
normal = report15s[report15s['df']==0.0] #.sample(len(default)*2)

from keras.models import Sequential #导入神经网络初始化函数
from keras.layers.core import Dense, Activation, Dropout #导入神经网络层函数、激活函数
#    dd = pd.DataFrame(Variance)
dd = default.merge(normal, how='outer')
dd=dd.sort_values(1,ascending=0)
nn_data = dd.as_matrix()
#shuffle(nn_data)
p = 0.8 # train/test ratio
m,n = np.shape(nn_data)
train = nn_data[:int(m*p),:]
test = nn_data[int(m*p):,:]
print(sum(train[:,-1]),sum(test[:,-1]))
netfile = './net2016.model' #构建的神经网络模型存储路径

net = Sequential() #建立神经网络
net.add(Dense(input_dim = n-1, output_dim = 64)) #添加输入层（3节点）到隐藏层（10节点）的连接
net.add(Activation('relu')) #隐藏层使用relu激活函数
#net.add(Dropout(0.5)) # over fitting
net.add(Dense(input_dim = 64, output_dim = 64)) #添加隐藏层1
net.add(Activation('relu')) #隐藏层使用relu激活函数
#net.add(Dropout(0.5)) # over fitting
net.add(Dense(input_dim = 64, output_dim = 16)) #添加隐藏层2
net.add(Activation('relu')) #隐藏层使用relu激活函数
##net.add(Dropout(0.5)) # over fitting
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
net.fit(train[:,:n-1], train[:,n-1], nb_epoch=250, batch_size=1) #训练模型，循环1000次
#net.save_weights(netfile) #保存模型
predict_result = net.predict_classes(test[:,:n-1]).reshape(len(test)) #预测结果变形
net.evaluate(train[:,:n-1], train[:,n-1])
import cm_plot  #导入自行编写的混淆矩阵可视化函数
cm_plot.cm_plot(test[:,n-1], predict_result).show()


import roc_plot
roc_plot.roc_plot(n,net,test).show()

predict_result16 = net.predict_classes(report16s.as_matrix()).reshape(len(report16s))
d2016=pd.DataFrame([code16.values,predict_result16],['code','df']).T
#print(d2016[d2016['df']==1])
#query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20141231"}]},{'_id':0,'COMP_NAME':1,'code':1})

industry = d2016.merge(get_issuer(),on='code')
df=industry[industry['df']==1]['COMP_NAME']
print(df)

from keras.utils import plot_model
plot_model(net, to_file='model.png')

from IPython.display import SVG
from keras.utils.vis_utils import model_to_dot
SVG(model_to_dot(net).create(prog='dot',format='svg'))
plot_model(net, to_file='model1.png')
