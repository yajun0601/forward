#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 14 16:00:36 2017

@author: zhengyajun
"""
import pandas as pd
import numpy  as np
import json
from pymongo import *
from numpy import *


client = MongoClient("mongodb://127.0.0.1:27017/")
db = client.bonds

def loadDataSet(fileName, delim='\t'):
    fr = open(fileName)
    stringArr = [line.strip().split(delim) for line in fr.readlines()]
    datArr = [map(float,line) for line in stringArr]
    return mat(datArr)

def pca(dataMat, topNfeat=9999999):
    meanVals = mean(dataMat, axis=0)
    meanRemoved = dataMat - meanVals #remove mean
    covMat = cov(meanRemoved, rowvar=0)
    eigVals,eigVects = linalg.eig(mat(covMat))
    eigValInd = argsort(eigVals)            #sort, sort goes smallest to largest
    eigValInd = eigValInd[:-(topNfeat+1):-1]  #cut off unwanted dimensions
    redEigVects = eigVects[:,eigValInd]       #reorganize eig vects largest to smallest
    lowDDataMat = meanRemoved * redEigVects#transform data into new dimensions
    reconMat = (lowDDataMat * redEigVects.T) + meanVals
    return lowDDataMat, reconMat

def replaceNanWithMean(): 
    datMat = loadDataSet('secom.data', ' ')
    numFeat = shape(datMat)[1]
    for i in range(numFeat):
        meanVal = mean(datMat[nonzero(~isnan(datMat[:,i].A))[0],i]) #values that are not NaN (a number)
        datMat[nonzero(isnan(datMat[:,i].A))[0],i] = meanVal  #set NaN values to mean
    return datMat

def sigmoid(inX):
    return float(1.0/(1+exp(-inX)))

def stocGradAscent1(dataMatrix, classLabels, numIter=150):
    m,n = shape(dataMatrix)
    weights = ones(n)   #initialize to all ones
    for j in range(numIter):
        dataIndex = list(range(m))
        for i in range(m):
            alpha = 4/(1.0+j+i)+0.0001    #apha decreases with iteration, does not 
            randIndex = int(random.uniform(0,len(dataIndex)))#go to 0 because of the constant
#            print(randIndex, sum(dataMatrix[randIndex]*weights))

            h = sigmoid(sum(dataMatrix[randIndex]*weights))
            error = classLabels[randIndex] - h
            weights = weights + alpha * error * dataMatrix[randIndex]
            del(dataIndex[randIndex])
    return weights

def classifyVector(inX, weights):
    prob = sigmoid(sum(inX*weights))
    if prob > 0.5:
        return 1.0
    else: return 0.0

def prob(inX, weights):
    prob = (sum(inX*weights))
    return prob

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
    
    defaults = (OCFTOINTEREST.union(OCFTOQUICKDEBT).union(OCFTOSHORTDEBT))
    df_code = np.array(list(defaults))
    df = np.ones(len(df_code))
    samples = pd.DataFrame([df_code,df], ['code','df'])
    samples = samples.T
    
    print('真实违约与技术性违约的交集')
    print((samples.merge(real_df_manufacture,on='code')))
    real_df_manufacture.pop('COMP_NAME')
    samples = samples.append(real_df_manufacture,ignore_index=True)
    return samples,real_df_manufacture

default,real_df_manu = default_sample()
query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20141231"}]},{'_id':0,'COMP_NAME':0,'LISTINGORNOT':0,'INDUSTRY_CSRC12':0,'INDUSTRY_CSRCCODE12':0,'NATURE':0,'rptDate':0,'PROVINCE':0})


first_defaults = db.default.aggregate([{'$group' : {'_id' : "$发行人", 'date' : {'$first':"$发生日期"}}}])
first_defaults = pd.DataFrame(list(first_defaults))

query = db.default_ratios.find({'$and' :[{'INDUSTRY_CSRCCODE12':'C'},{"rptDate" : "20141231"}]},{'_id':0,'code':1})
manufactures = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_balance.find({"rptDate" : "20141231"},{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0, 'rptDate':0})
balance = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_cashflow.find({"rptDate" : "20141231"},{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0,'rptDate':0})
cashflow = pd.DataFrame(list(query)) #Convert the input to an array.

query = db.bond_profit.find({"rptDate" : "20141231"},{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0,'rptDate':0})
profit = pd.DataFrame(list(query)) #Convert the input to an array.

tmp = balance.merge(cashflow, on='code')
finacial_report = tmp.merge(profit, on='code')  # finacial report of 2015

#import numpy as np
#from sklearn.preprocessing import Imputer
##均值填补NaN  axis=0是行 1是列
#imp = Imputer(missing_values='NaN', strategy='mean', axis=1)
#value = finacial_report.values
#imp.fit(value)
#imp = Imputer(missing_values='nan, strategy='mean', axis=0)
# 整合成一张大表，标注 'df' 筛选出制造业 为 0 和 1

manu_report = manufactures.merge(finacial_report,on='code')
code = manu_report.pop('code')

report = manu_report.fillna(manu_report.mean())
report['code'] = code
report.dropna(axis=1,how='any',inplace=True)

report = report.merge(default, how='left',on='code')
report = report.fillna(0)

#insert_record = json.loads(report.to_json(orient='records'))
#ret = db.manufacture2015.insert_many(insert_record)
#replace : boolean, optional do not replace
code = report.pop('code')
df = report.pop('df')
from sklearn.preprocessing import StandardScaler
standard =  StandardScaler().fit_transform(report.values)
report = pd.DataFrame(standard)
report['df'] = df
default = report[report['df']==1.0]
normal = report[report['df']==0.0]#.sample(len(default)*2)

#def LogisticRegression(result):
data = default.merge(normal, how='outer') # trainning samples


# 归一化
df_flag = data.pop('df')
#from sklearn.preprocessing import StandardScaler
#X = StandardScaler().fit_transform(data.values)

trainWeights = stocGradAscent1(data.values, df_flag.values, 5000)
normal = report[report['df']==0.0]#.sample(len(default)*2)
testSet = default.merge(normal, how='outer')

default_num = len(testSet[testSet['df'] == 1.0])
normal_num = len(testSet[testSet['df'] == 0.0])
testLabels = testSet.pop('df')
Normal_errorCount = 0;Default_errorCount = 0
    
index = 0
for line in testSet.values:
    index += 1
    df = testLabels.values[index - 1]
    if int(classifyVector(line.astype('float64'), trainWeights))!= int(df):
        if int(df) == 0:
            Normal_errorCount += 1
        else:
            Default_errorCount += 1
            
default_errorRate = (float(Default_errorCount)/default_num)
normal_errorRate = (float(Normal_errorCount)/normal_num)
print("default_num: %d,normal_num: %d"%(default_num, normal_num))
print("default_errorRate: %d : %f, normal_errorRate:%d : %f" %(Default_errorCount,default_errorRate,Normal_errorCount,normal_errorRate))


df = report.pop('df')
zzz = pd.DataFrame()
for ii in range(len(report)):    
    value=sum(report.values[ii]*trainWeights)
#    print(ii,manu)
    zzz = zzz.append({'sum':value}, ignore_index=True)

zzz['code']=code
zzz['df']=df

doc = zzz.sort_values('sum')
doc = doc.reset_index(drop=True)
print(doc[doc['df']==1.0])

doc = doc.merge(real_df_manu,on='code',how='left')
print(doc[doc['df_y']==1.0])
doc.to_csv("result0516.csv")


import matplotlib.pyplot as plt

x = list(doc[doc['df_x']==1.0].index)
plt.hist(x,10)
plt.show()

#from sklearn.linear_model import LogisticRegression as LR
#from sklearn.linear_model import RandomizedLogisticRegression as RLR 
#
#x = X #data.values
#y = df_flag.values
#rlr = RLR() #建立逻辑回归模型，筛选变量
#rlr.fit(x, y) #训练模型
#rlr.get_support() #获取特征筛选结果，也可以通过
#print(u'通过随机逻辑回归模型筛选结束')
#print(u'有效特征为：%s' % ','.join(data.columns[rlr.get_support()]))
#x = data[data.columns[rlr.get_support()]].as_matrix() # 
#
#lr = LR() # 建立逻辑回归模型
#lr.fit(x, y) # 用筛选后的特征数据来训练模型
#print(u'训练结束')
#print(u'平均正确率为：%s' % lr.score(x, y))
#



