#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 15:50:24 2017

@author: yajun

1. import all the default bonds
"""

import pandas as pd
import numpy as np
import json

client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")

db = client.stocks

query_value = list(db.bond.distinct("comp_name"))
comp_list = pd.Series(query_value)


db = client.bonds
DATA_FILE='../data/违约债券报表.xlsx'
df = pd.read_excel(DATA_FILE,sheetname=[0], header = 0)[0]
'''
insert_record = json.loads(df.to_json(orient='records'))
ret = db.default.insert_many(insert_record)
print(ret)

Index(['代码', '名称', '发生日期', '事件摘要', '发行人', '担保人', '最新债项评级', '最新主体评级', '发行时主体评级',
       '评级历史', '债券余额', '票面利率', '公司属性', '起息日期', '到期日期', '主承销商', '是否上市公司', '省份',
       '所属wind行业', 'Wind债券二级分类', '上市交易所', '发行时债项评级'],
      dtype='object')
'''
#default = df[['发行人','发生日期', '最新主体评级', '发行时主体评级','是否上市公司', '省份', '所属wind行业']]
default = df[['发行人', '是否上市公司', '省份', '所属wind行业']]
default = default.sort_values(by='发行人', ascending=False)
default = default.fillna(0)
default = default.drop_duplicates()
default['df'] = 1
comp_df = pd.DataFrame(data=comp_list, columns=['发行人'])

result = pd.concat([default,comp_df])
result['df'] = result['df'].fillna(0)

insert_record = json.loads(result.to_json(orient='records'))
ret = db.issuers.insert_many(insert_record)
print(ret)