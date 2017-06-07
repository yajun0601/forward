#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  5 10:10:13 2017

@author: zhengyajun
"""


import pandas as pd     
from numpy  import * 
from pymongo import *
import json
    
    
    dest_client = MongoClient('mongodb://127.0.0.1:27017/')
    dest_db = dest_client.bonds
    dest_data = dest_db.issuers
    COMMON_RATIO = pd.DataFrame()
    balance_data = pd.DataFrame(list(dest_db.bond_balance.find()))
    
    data = balance_data[balance_data['rptDate'] == '20151231']
    data = data.drop(labels=['_id','COMP_NAME'],axis=1)
    data['DEBTTOASSETS'] = data['TOT_LIAB']/data['TOT_ASSETS']

default = pd.DataFrame(list(dest_db.issuers_info.find({})))
default = default.drop(labels=['NATURE','_id','CITY','PROVINCE','LISTINGORNOT'],axis=1)
df = default[default['df']==1.0]

result = df.merge(data, how='inner', on='code')
result = result.dropna(axis=1,how='all')
result.to_csv('default_balance.csv')
#########################################
data = pd.DataFrame(list(dest_db.bond_cashflow.find()))
data = data[data['rptDate'] == '20151231']
data = data.drop(labels=['_id','COMP_NAME'],axis=1)
df = default[default['df']==1.0]
result = df.merge(data, how='inner', on='code')
result = result.dropna(axis=1,how='all')
result.to_csv('default_cashflow.csv')
###################################
data = pd.DataFrame(list(dest_db.bond_profit.find()))
data = data[data['rptDate'] == '20151231']
data = data.drop(labels=['_id','COMP_NAME'],axis=1)
df = default[default['df']==1.0]
result = df.merge(data, how='inner', on='code')
result = result.dropna(axis=1,how='all')
result.to_csv('default_profit.csv')

