#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 14:14:47 2017

@author: yajun
"""


import pandas as pd
import numpy  as np
from pymongo import *
import json
from bson.son import SON

def create_database():
    client = MongoClient("mongodb://192.168.10.60:27017/")
    db = client.companies
    
    query = db.JudgementDetail.find({'JudgeDate':{'$exists': 1 }, 'Defendantlist':{'$exists': 1 }})
    data = pd.DataFrame(list(query))
    print(data['Defendantlist'])
    
    # read all the issures name
    
    bond_db = client.bonds
    query = bond_db.issuers_info.distinct('COMP_NAME')
    #query = bond_db.issuers_info.find()
    issuers = set(list(query))
    
    # merge new dataframe
    df = pd.DataFrame()
    for index in range(len(data)): #len(data)
        record = data['Defendantlist'].values[index]
        inter = issuers.intersection(set(record))
        if inter : #  empty set
    #        print(inter)     
            df = df.append(data.iloc[index], ignore_index=True)
        else:
            continue
    
    # prepare year and date
    import datetime
    
    judgeDate = df['JudgeDate']
    year = list()
    quarter = list()
    for index in range(len(judgeDate)):
        dt64 = np.datetime64(judgeDate.values[index])
        dtime = dt64.astype('M8[M]').astype('O')
        year.append(dtime.year)
        
        if dtime.month in range(1,4):
            quarter.append(1)
        elif dtime.month in range(4,7):
            quarter.append(2)
        elif dtime.month in range(7,10):
            quarter.append(3)
        elif dtime.month in range(10,13):
            quarter.append(4)
            
    #print(year, quarter)
    df['year'] = year
    df['quarter'] = quarter
    
    insert_record = json.loads(df.to_json(orient='records'))
    db = client.bonds
    
    ret = bond_db.quarter_judgements.insert_many(insert_record)
    print(ret)
  
  
  
  