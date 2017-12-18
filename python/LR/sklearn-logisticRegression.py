#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 09:12:35 2017

@author: yajun
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cross_validation import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report 
from sklearn.metrics import precision_recall_curve, roc_curve, auc 
from pymongo import MongoClient



from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import minmax_scale
from sklearn.preprocessing import MaxAbsScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing.data import QuantileTransformer



client = MongoClient("mongodb://127.0.0.1:27017/")
#client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
db = client.bonds

query = db.ytm_std_samples.find({},{'_id':0})
record = pd.DataFrame(list(query))
client.close()
    
record['score1'] = record['defendant'] + record['shixin'] + record['zhixing'] 
record['score2'] = record['std']


record.rename(columns={'df':'result'},inplace=True)
sample = record[['score1','score2']]

standard =  StandardScaler().fit_transform(sample.values)
data = pd.DataFrame(standard)
data.rename(columns={0:'score1',1:'score2'},inplace=True)
data['result'] = record['result']

data.drop(5,inplace=True)
data.drop(3,inplace=True)


#data = pd.read_csv('ex2data1.txt', sep=',', \
#    skiprows=[2], names=['score1','score2','result'])
score_data = data.loc[:,['score1','score2']]
result_data = data.result

p = 0
for i in range(10):
    x_train, x_test, y_train, y_test = \
        train_test_split(score_data, result_data, test_size = 0.2)
    model = LogisticRegression(C=1e9)
    model.fit(x_train, y_train)
    predict_y = model.predict(x_test)
    p += np.mean(predict_y == y_test)

# 绘制图像
pos_data = data[data.result == 1].loc[:,['score1','score2']]
neg_data = data[data.result == 0].loc[:,['score1','score2']]

h = 0.02
x_min, x_max = score_data.loc[:, ['score1']].min() - .5, score_data.loc[:, ['score1']].max() + .5
y_min, y_max = score_data.loc[:, ['score2']].min() - .5, score_data.loc[:, ['score2']].max() + .5
xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))
Z = model.predict(np.c_[xx.ravel(), yy.ravel()])

# 绘制边界和散点
Z = Z.reshape(xx.shape)
plt.pcolormesh(xx, yy, Z, cmap=plt.cm.Paired)
plt.scatter(x=pos_data.score1, y=pos_data.score2, color='black', marker='o')
plt.scatter(x=neg_data.score1, y=neg_data.score2, color='red', marker='*')

plt.xlim(xx.min(), xx.max())
plt.ylim(yy.min(), yy.max())
plt.show()

# 模型表现
answer = model.predict_proba(x_test)[:,1]  
precision, recall, thresholds = precision_recall_curve(y_test, answer)      
report = answer > 0.5  
print(classification_report(y_test, report, target_names = ['neg', 'pos']))  
print("average precision:", p/100)  