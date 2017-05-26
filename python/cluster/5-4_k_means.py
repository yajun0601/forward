#-*- coding: utf-8 -*-
#使用K-Means算法聚类特征数据

import pandas as pd
import numpy  as np
import json
from pymongo import *
from numpy import *


client = MongoClient("mongodb://127.0.0.1:27017/")
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
manu_report = manufactures.merge(finacial_report,on='code')
code = manu_report.pop('code')

report = manu_report
#report['code'] = code
report.dropna(axis=1,how='any', thresh= 100,inplace=True)
report.fillna(report.mean())
#report = report.merge(default, how='left',on='code')
#report = report.fillna(0)

#参数初始化
#inputfile = '../data/consumption_data.xls' #销量及其他属性数据
outputfile = './tmp/data_type.xls' #保存结果的文件名
k = 2 #聚类的类别
iteration = 500 #聚类最大循环次数
#data = pd.read_excel(inputfile, index_col = 'Id') #读取数据
data = report.fillna(report.mean())
data=data.set_index(code)
data_zs = 1.0*(data - data.mean())/data.std() #数据标准化

from sklearn.cluster import KMeans
model = KMeans(n_clusters = k, n_jobs = 8, max_iter = iteration) #分为k类，并发数4
model.fit(data_zs) #开始聚类

#简单打印结果
r1 = pd.Series(model.labels_).value_counts() #统计各个类别的数目
r2 = pd.DataFrame(model.cluster_centers_) #找出聚类中心
r = pd.concat([r2, r1], axis = 1) #横向连接（0是纵向），得到聚类中心对应的类别下的数目
r.columns = list(data.columns) + [u'类别数目'] #重命名表头
print(r)
r.to_excel("./tmp/classes.xls")

#详细输出原始数据及其类别
r = pd.concat([data, pd.Series(model.labels_, index = data.index)], axis = 1)  #详细输出每个样本对应的类别
r.columns = list(data.columns) + [u'聚类类别'] #重命名表头
r.to_excel(outputfile) #保存结果


def density_plot(data): #自定义作图函数
  import matplotlib.pyplot as plt
  plt.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
  plt.rcParams['axes.unicode_minus'] = False #用来正常显示负号
  p = data.plot(kind='kde', linewidth = 2, subplots = True, sharex = False)
  [p[i].set_ylabel('密度') for i in range(3)]
  plt.legend()
  return plt

#pic_output = './tmp/pd_' #概率密度图文件名前缀
#for i in range(k):
#  density_plot(data[r['聚类类别']==i]).savefig(u'%s%s.png' %(pic_output, i))
  
  
from sklearn.manifold import TSNE

tsne = TSNE()
tsne.fit_transform(data_zs) #进行数据降维
tsne = pd.DataFrame(tsne.embedding_, index = data_zs.index) #转换数据格式

import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False #用来正常显示负号

#不同类别用不同颜色和样式绘图
d = tsne[r[u'聚类类别'] == 0]
plt.plot(d[0], d[1], 'r.')
d = tsne[r[u'聚类类别'] == 1]
plt.plot(d[0], d[1], 'go')
d = tsne[r[u'聚类类别'] == 2]
plt.plot(d[0], d[1], 'b*')
#d = tsne[r[u'聚类类别'] == 3]
#plt.plot(d[0], d[1], 'k*')
plt.show()
