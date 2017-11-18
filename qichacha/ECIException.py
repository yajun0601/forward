#!/bin/python
#coding=utf-8

import numpy as np
import pandas as pd
import os,time,sys,re
import csv
import scipy
import smtplib

#from pymongo import *
import json
from datetime import datetime,timedelta
from pymongo import *
import urllib.request
import sys

DATA_FILE='data/bond_info.xlsx'
#ApiKey='ApiKey'
ApiKey = '4cfc602109f24541ac70e1a904115ad1'
def load(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
        print(data)
        return data
'''
请求示例：http://i.yjapi.com/Judicial/SearchCourtAnnouncement?key=ApiKey&companyName=小米科技有限责任公司
companyName String  是   公司名字
pageSize    int     否   每页条数，默认10条，最大不超过20条
pageIndex   int     否   页码，默认第1页
key         String  是   接口ApiKey
dtype       String  否   返回数据格式：json或xml，默认json
'''
def CourtAnnouncement():
    filename = 'CourtAnnouncement.json'
    info = load(filename)


    return info

def database():
    client = MongoClient()
    client = MongoClient("mongodb://localhost:27017/")
    db = client.companies
    collection = db.company
#    collection.insert(json.loads(df.to_json(orient='records')))    
    for i in range(len(record)):
        company = record[i]
        company['_id']=company['KeyNo'] 
        rec = collection.find({'KeyNo':company['KeyNo']})
        if rec.count() > 0: # if the id exist, let's check the UpdateTime
            if rec[0]['UpdatedDate'] >= company['UpdatedDate']: 
                continue    #if the record in database is the latest, exit
            else:           #else, replace with the latest
                collection.replace_one({'KeyNo':company['KeyNo']},company)
                continue
       
        print(i,company)
        collection.insert_one(company)


def OpException(keyNo):
    '''
    请求示例：http://i.yjapi.com/ECIException/GetOpException?key=ApiKey&keyNo=ac66f9d18c12d884e23b7c83bbad1c6b 
    名称      类型  是否必填    描述
    keyNo   String  是   公司KeyNo
    key     String  是   接口ApiKey
    dtype   String  否   返回数据格式：json或xml，默认json

    filename = 'JudgementDetail.json'
    ret = load(filename)
    '''    
    DEV_URL='http://dev.i.yjapi.com/ECIException/GetOpException?key=ApiKey=%s&keyNo=%s'%(ApiKey,keyNo)
    BASE_URL='http://i.yjapi.com/ECIException/GetOpException?key=ApiKey=%s&keyNo=%s'%(ApiKey,keyNo)
    url = DEV_URL
    print(url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
#    print(ret)
    if ret['Status'] == '200':
        detail = ret['Result']
        detail['_id'] = detail['Id']
        
        jd_collection = db.JudgementDetail
        in_collection = jd_collection.find_one({'_id':detail['_id']})
        if in_collection is None:   # only if the id does not exist
            print('insert detail:' + detail['_id'])
            jd_collection.insert_one(detail)
        # TODO: if the record need to be updated?
        return detail
    else:
        print(ret)
        return None

from urllib import parse

def JudgmentDoc(company):
    '''
    请求示例：http://i.yjapi.com/Judicial/SearchJudgmentDoc?key=ApiKey&searchKey=小米科技有限责任公司
    请求参数：
    searchKey   String  是   查询关键字
    isExactlySame   String  否   名称是否需要与关键字一样,True或者False(Default value is False)
    caseType    String  否   案件类型(ms:民事 xs:刑事 xz:行政 zscq:知识产权 pc:赔偿 zx:执行)默认为空
    pageSize    int     否   每页条数，默认10条，最大不超过20条
    pageIndex   int     否   页码，默认第1页
    key         String  是   接口ApiKey
    dtype       String  否   返回数据格式：json或xml，默认json
    
    filename = 'JudgmentDoc.json'
    ret = load(filename)
    '''
    DEV_URL='http://dev.i.yjapi.com/Judicial/SearchJudgmentDoc?key=ApiKey=%s&searchKey='%(ApiKey)
    BASE_URL='http://i.yjapi.com/Judicial/SearchJudgmentDoc?key=ApiKey=%s&searchKey='%(ApiKey)

    url = DEV_URL + urllib.parse.quote(company) # Chinese characters need to be quoted
    print("url:" + url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
        
    if ret['Status'] == '200':
        detail = ret['Result']
        doc_collection = db.JudgmentDoc
        for i in range(len(detail)):
            detail[i]['_id'] = detail[i]['Id']
            in_doc = doc_collection.find_one({'_id':detail[i]['_id']})
            if in_doc is None:
                print("insert:" + detail[i]['_id'])
                doc_collection.insert_one(detail[i])
            elif in_doc['UpdateDate'] < detail[i]['UpdateDate']:
                print("update:"+detail[i]['_id'])
                doc_collection.find_one_and_replace({'_id':detail[i]['_id']},detail[i])
        
        return detail
    else:
        print(ret)
    return None

def company_list(data_file):
    bond_info = pd.read_excel(data_file,sheetname=[1], header = 0)
    df = bond_info[1]
    cmp_list = df[df.keys()[3]].tolist()
    return cmp_list

client = MongoClient()
client = MongoClient("mongodb://localhost:27017/")
db = client.companies
if __name__ == "__main__":
    '''
    detail = JudgementDetail()
    if detail is None:
        print("detail is None")
    else:
        print(detail['CaseName'])

    filename = 'JudgmentDoc.json'
    Doc = JudgmentDoc(filename)
    '''    
# get company list
    company_list = company_list(DATA_FILE)
    for company in company_list[:2]:
        print(company)        
        jd_doc = JudgmentDoc(company)
        jd_detail = JudgementDetail(jd_doc[0]['Id'])
        print(jd_detail['Id'])