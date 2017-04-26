#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 17:04:56 2017

@author: yajun
"""

import pandas as pd
import numpy as np
import math
from pymongo import *
import json
from bson.son import SON

client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.companies

#DATA_FILE='data/company_info.xls'
DATA_FILE='data/OD_CL_20170321151347smSUW_0.xls'
#df = pd.read_excel(DATA_FILE,sheetname=None)

def company_info():
    company_info = pd.read_excel(DATA_FILE,sheetname=[1], header = 0)
    company_info_df=company_info[1]
    company_info_df['_id'] = company_info_df['ID']
    insert_record = json.loads(company_info_df.to_json(orient='records'))
    
    ret = db.company.insert_many(insert_record)
    return ret

def people():
    data = pd.read_excel(DATA_FILE,sheetname=[2], header = 0)
    df = data[2]
    #    df['_id'] = df['ID']
    insert_record = json.loads(df.to_json(orient='records'))
    
    ret = db.people.insert_many(insert_record)
    return ret

def changes_record():
    data = pd.read_excel(DATA_FILE,sheetname=[3], header = 0)
    df = data[3]
    #    df['_id'] = df['ID']
    insert_record = json.loads(df.to_json(orient='records'))
    
    ret = db.changes_record.insert_many(insert_record)
#    return ret
def branches():
    data = pd.read_excel(DATA_FILE,sheetname=[4], header = 0)
    df = data[4]
    #    df['_id'] = df['ID']
    insert_record = json.loads(df.to_json(orient='records'))
    
    ret = db.branches.insert_many(insert_record)
#    return ret
def ECIException():
    index = 5
    data = pd.read_excel(DATA_FILE,sheetname=[index], header = 0)
    df = data[index]
    #    df['_id'] = df['ID']
    insert_record = json.loads(df.to_json(orient='records'))
    
    ret = db.ECIException.insert_many(insert_record)
#    return ret
def owner_info():
    index = 6
    data = pd.read_excel(DATA_FILE,sheetname=[index], header = 0)
    df = data[index]
    #    df['_id'] = df['ID']
    insert_record = json.loads(df.to_json(orient='records'))
    
    ret = db.owner_info.insert_many(insert_record)
#    return ret
def industry_class():
    index = 7
    data = pd.read_excel(DATA_FILE,sheetname=[index], header = 0)
    df = data[index]
    df['_id'] = df['ID']
    insert_record = json.loads(df.to_json(orient='records'))
    
    ret = db.industry_class.insert_many(insert_record)
#    return ret
def shixin():
    shname = '失信信息'
    data = pd.read_excel(DATA_FILE,sheetname=[index], header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    
    ret = db.shixin.insert_many(insert_record)
#    return ret
def zhixing():
    shname = '执行信息'
    data = pd.read_excel(DATA_FILE,sheetname=shname, header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    ret = db.zhixing.insert_many(insert_record)
#    return ret    
def judgmentDoc():
    shname = '裁判文书'
    data = pd.read_excel(DATA_FILE,sheetname=shname, header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    ret = db.JudgmentDoc.insert_many(insert_record)
#    return ret
def patent():
    shname = '专利信息'
    data = pd.read_excel(DATA_FILE,sheetname=shname, header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    ret = db.patent.insert_many(insert_record)
#    return ret
def news():
    shname = '新闻信息'
    data = pd.read_excel(DATA_FILE,sheetname=shname, header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    ret = db.patent.insert_many(insert_record)
#    return ret
    
def Recruitment():
    shname = '招聘信息'
    data = pd.read_excel(DATA_FILE,sheetname=shname, header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    ret = db.Recruitment.insert_many(insert_record)
#    return ret
def personal_investment():
    shname = '个人对外投资信息'
    data = pd.read_excel(DATA_FILE,sheetname=shname, header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    ret = db.personal_investment.insert_many(insert_record)
#    return ret
    
if __name__ == "__main__":
    shname = '专利信息(接上页-2)'
    data = pd.read_excel(DATA_FILE,sheetname=shname, header = 0)
    #df = data[index]
#    df['_id'] = df['ID']
    insert_record = json.loads(data.to_json(orient='records'))
    ret = db.patent.insert_many(insert_record)
