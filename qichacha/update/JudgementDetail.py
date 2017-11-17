#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 14:44:07 2017

@author: yajun
"""
import numpy as np
import pandas as pd
import sys
import json
from datetime import datetime,timedelta
from pymongo import MongoClient
import pymongo
import urllib.request
from urllib import parse

client = MongoClient("mongodb://localhost:27017/")
db = client.qichacha_new


err_code = ['101','102','103','104','105','107','108','109','110','199']

DEV = 0
if DEV == 1 :
    DATA_FILE='data/bond_info.xlsx'
    ApiKey='ApiKey'
    BASE_URL='http://dev.i.yjapi.com/'
else:
    ApiKey = 'f104ea4923df4c58b75f3b37f8c47329'
    BASE_URL='http://i.yjapi.com/'
    
    
def JudgementDetail(Id):
    '''
    请求示例：http://i.yjapi.com/JudgeDocV4/GetJudgementDetail?key=AppKey&id=48a02bc8ef08fdbe51bf842de318c438
    id      String  是   内部主键
    key     String  是   应用APPKEY(应用详细页查询)
    dtype   String  否   返回数据格式：json或xml，默认json

    filename = 'JudgementDetail.json'
    ret = load(filename)
    '''    
#    DEV_URL='http://dev.i.yjapi.com/Judicial/GetJudgementDetail?key=ApiKey=%s&id=%s'%(ApiKey,Id)
#    BASE_URL='http://i.yjapi.com/Judicial/GetJudgementDetail?key=ApiKey=%s&id='%(ApiKey)
    url = BASE_URL+'JudgeDocV4/GetJudgementDetail?key=%s&id=%s'%(ApiKey,Id)

    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] == '200':
        detail = ret['Result']
        detail['_id'] = detail['Id']
        
        jd_collection = db.JudgementDetail
        in_collection = None
        if jd_collection.name in db.collection_names():
            in_collection = jd_collection.find_one({'_id':detail['_id']})
        if in_collection is None:   # only if the id does not exist
#            print('insert detail:' + detail['_id'])
            jd_collection.insert_one(detail)
        # TODO: if the record need to be updated?
    print(ret['Status'])
    return ret['Status']

def getJudgementDetail():
    jd_collection = db.JudgmentDoc_isExactlySame
    query = jd_collection.find({},{'_id':0,'Id':1}).sort("Id",pymongo.ASCENDING)
    idList = list(query)
    length = len(idList)
    
    collection = db.JudgementDetail
    query = collection.find({},{'_id':0,'Id':1}).sort("_id",pymongo.DESCENDING).limit(3)
    record_id = list(query)
    print(record_id)
    
    ii = idList.index(record_id[0])
    for i in range(ii+1,ii+30000):
        print("%d/%d\t%s"%(i,length,idList[i]['Id']))
        ret = JudgementDetail(idList[i]['Id'])
        if ret in err_code:
            print('err_code: %s'%ret)
            break
    return ret        
    
   
      
if __name__ == "__main__":
    getJudgementDetail()