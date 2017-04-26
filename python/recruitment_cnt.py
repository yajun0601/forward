#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 14:42:37 2017

@author: yajun
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 11:54:51 2017

@author: yajun
"""

import pandas as pd
import numpy  as np
from pymongo import *
import json
from bson.son import SON

client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.companies

#input_data = db.JudgementDetail
#data = pd.DataFrame(list(input_data.find()))
# from 201604 
columns = ['2016Q2','2016Q3','2016Q4','2017Q1']
#columns = ['2016Q1','2016Q2']
datelist = ['2016-03-31','2016-06-30', '2016-09-30','2016-12-31','2017-03-31']
dflist = list()
for ii in range(len(columns)):
    print(columns[ii], datelist[ii], datelist[ii + 1])
    pipeline =   [
        { "$match": { "发布时间": { "$gt": datelist[ii], "$lte": datelist[ii + 1] } } },
        { "$group": {"_id": "$公司名", columns[ii]: { "$sum": 1 } }},
        { "$sort": SON([("count", -1)])}
    ]
    '''
            { "$match": { "发布时间": { "$gt": '2016-10-11', "$lte": '2016-12-11' } } },
        { "$group": {"_id": "$公司名", sum: { "$sum": 1 } }}
    '''
    query_value = list(db.Recruitment.aggregate(pipeline))
    tmp_df = pd.DataFrame(query_value)
    indexs = tmp_df['_id']
    count = tmp_df[columns[ii]].values
    df = pd.DataFrame(data = count, index = indexs, columns = [columns[ii]])          
    dflist.append(df)
#db.command('aggregate', 'JudgementDetail', pipeline=pipeline, explain=True)
result=pd.concat(dflist,axis=1)
result['company'] = result.index.values
insert_record = json.loads(result.to_json(orient='records'))
ret = db.recruitment_count.insert_many(insert_record)
print(ret)


'''

df[columns[0]] = values[1]
import pprint
pprint.pprint(values)

df1=DataFrame({'2016Q1':[1,'A5','A6','A7']},
               index=['a','b','c','d'])
df2=DataFrame({'2016Q2':[1,'A6','A7']},
               index=['a','c','d'])
'''
