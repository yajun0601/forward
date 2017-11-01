#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 18:34:55 2017

@author: yajun
"""

#!/bin/python
#coding=utf-8

import numpy as np
import pandas as pd
import sys

#from pymongo import *
import json
from datetime import datetime,timedelta
from pymongo import MongoClient
import urllib.request
import time
import random
import math

client = MongoClient("mongodb://localhost:27017/")
db = client.qichacha
filename = '平安信托-SW综合-公司列表.xlsx'
issuers = pd.read_excel('平安信托-SW综合-公司列表.xlsx',sheetname=[0], header = 0)[0]

DEV = 0
if DEV == 1 :
    DATA_FILE='data/bond_info.xlsx'
    ApiKey='ApiKey'
    BASE_URL='http://dev.i.yjapi.com/'
else:
    ApiKey = '4cfc602109f24541ac70e1a904115ad1'
    BASE_URL='http://i.yjapi.com/'
    
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
def CourtAnnouncement(comp_name):
#    comp_name = '北京京东世纪贸易有限公司'
    collection = db.Announcement
    url = BASE_URL+'Judicial/SearchCourtAnnouncement?key=%s&companyName='%(ApiKey)
    url = url + urllib.parse.quote(comp_name) # Chinese characters need to be quoted
    print(url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    print(ret)
    if ret['Status'] != '200':
        print(ret)
    else:
        paging=ret['Paging']
        if paging['TotalRecords'] > 0:
            result = ret['Result']
            for i in range(paging['TotalRecords']):
                dict = result[i]
                # insert into mongodb
                rec = collection.find({'Content':dict['Content']}) # the same content, means repeated
                if rec.count() == 0 :
                    collection.insert_one(dict)
#                df = pd.DataFrame.from_dict(dict, orient='index')
           
    return ret
def dump_Announcement():
    collection = db.Announcement
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('CourtAnnouncement.xlsx')

def CourtAnnouncement1():
    filename = 'CourtAnnouncement.json'
    info = load(filename)
    return info

def database():
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


def JudgementDetail_old(Id):
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
    url = BASE_URL+'Judicial/GetJudgementDetail?key=%s&id=%s'%(ApiKey,Id)
    print(url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
#    print(ret)
    if ret['Status'] != '200':
        print(ret)
    else:
        detail = ret['Result']
        detail['_id'] = detail['Id']
        
        jd_collection = db.JudgementDetail
        in_collection = None
        if jd_collection.name in db.collection_names():
            in_collection = jd_collection.find_one({'_id':detail['_id']})
        if in_collection is None:   # only if the id does not exist
            print('insert detail:' + detail['_id'])
            jd_collection.insert_one(detail)
        # TODO: if the record need to be updated?
        return detail
    return ret

def dump_JudgementDetail():
    collection = db.JudgementDetail
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('JudgementDetail.xlsx')
from urllib import parse

def JudgmentDoc(company, PageIndex=1):
    '''
    请求示例：http://i.yjapi.com/JudgeDocV4/SearchJudgmentDoc?key=ApiKey&searchKey=北京京东世纪贸易有限公司
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
#    DEV_URL='http://dev.i.yjapi.com/Judicial/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey)
#    BASE_URL='http://i.yjapi.com/Judicial/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey)
    collection = db.JudgmentDoc_isExactlySame
    url = BASE_URL + 'Judicial/SearchJudgmentDoc?isExactlySame=True&key=%s&searchKey='%(ApiKey) + urllib.parse.quote(company)+"&pageIndex=%d"%(PageIndex)  # Chinese characters need to be quoted
    print("%s url: \t%s"%(company,url))
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
        
    if ret['Status'] != '200':
        print(ret)
        return
    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = math.ceil(TotalRecords/PageSize)
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()*3 + 1
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            JudgmentDoc(company,PageIndex+1)
        else:
            print("finished %s"%company)
            return 
 
def JudgementDetail(id):
    collection = db.JudgementDetail    
    url = BASE_URL + 'Judicial/GetJudgementDetail?key=%s&id=%s'%(ApiKey,id)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
        
    if ret['Status'] == '200':
        detail = ret['Result']
        collection.insert_one(detail)
        
        
#company='北京京东世纪贸易有限公司'
#JudgmentDoc(company)
        
def JudgmentDoc_old(company, PageIndex=1):
    '''
    请求示例：http://i.yjapi.com/JudgeDocV4/SearchJudgmentDoc?key=ApiKey&searchKey=北京京东世纪贸易有限公司
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
#    DEV_URL='http://dev.i.yjapi.com/Judicial/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey)
#    BASE_URL='http://i.yjapi.com/Judicial/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey)

    url = BASE_URL + 'Judicial/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey) + urllib.parse.quote(company)+"&pageIndex=%d"%(PageIndex)  # Chinese characters need to be quoted
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
                JudgementDetail(detail[i]['_id'])               # insert judgement details
            elif in_doc['UpdateDate'] < detail[i]['UpdateDate']:
                print("update:"+detail[i]['_id'])
                doc_collection.find_one_and_replace({'_id':detail[i]['_id']},detail[i])
        
        return detail
    else:
        print(ret)
    return None
def dump_JudgmentDoc():
    collection = db.JudgmentDoc
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('JudgmentDoc.xlsx')

   
def company_list(data_file):
    bond_info = pd.read_excel(data_file,sheetname=[1], header = 0)
    df = bond_info[1]
    cmp_list = df[df.keys()[3]].tolist()
    return list(set(cmp_list))  # identical 

def SearchZhiXing(name, PageIndex=1):
    '''
    接口地址：http://i.yjapi.com/Court/SearchZhiXing
    支持格式：JSON/XML
    请求方式：GET
    请求示例：http://i.yjApi.com/Court/SearchZhiXing?key=ApiKey&searchKey=张三 '''
#    name='张三'
    collection = db.zhixing
    url = BASE_URL + 'Court/SearchZhiXing?isExactlySame=True&key=%s&searchKey='%(ApiKey) + urllib.parse.quote(name)+"&pageIndex=%d"%(PageIndex)
    print("name \t" + url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] != '200':
        print(ret)
    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = math.ceil(TotalRecords/PageSize)
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            SearchShiXin(name,PageIndex+1)
        else:
            print("finished %s"%name)
            return
            
            
            
def dump_zhixing():
    collection = db.zhixing
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('zhixing.xlsx')

    
def SearchShiXin(name,PageIndex=1):
    '''
    接口地址：http://i.yjapi.com/Court/SearchShiXin
    支持格式：JSON/XML
    请求方式：GET
    请求示例：http://i.yjApi.com/Court/SearchShiXin?key=ApiKey&searchKey=张三 '''
#    name='重庆市德感建筑安装工程有限公司'  # 23 items
    collection = db.shixin_new
    url = BASE_URL + 'Court/SearchShiXin?isExactlySame=True&key=%s&searchKey='%(ApiKey) + urllib.parse.quote(name) +"&pageIndex=%d"%(PageIndex)
    print("wget \t" + url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] != '200':
        print(ret)
    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = math.ceil(TotalRecords/PageSize)
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            SearchShiXin(name,PageIndex+1)
        else:
            print("finished %s"%name)
            return
        
#name='重庆市德感建筑安装工程有限公司'
#SearchShiXin(name, PageIndex=1)

            
def dump_shixin():
    collection = db.shixin
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('shixin.xlsx')
    
def Shixin_and_zhixing(name):
    '''
    接口地址：http://i.yjapi.com/Court/SearchCourt
    支持格式：JSON/XML
    请求方式：GET
    请求示例：http://i.yjApi.com/Court/SearchCourt?key=ApiKey&searchKey=张三 '''
#    name='张三'
    url = BASE_URL + 'Court/SearchCourt?key=%s&searchKey='%(ApiKey) + urllib.parse.quote(name)
    print("url:" + url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] != '200':
        print(ret)
    else:
        result = ret['Result']
        if result['ZhiXingResult']['Paging']['TotalRecords'] > 0 :
            zhixing_result = result['ZhiXingResult']['Items']
            collection = db.zhixing_result
            dbret = collection.insert_many(zhixing_result)
            print(dbret)
        if result['ShiXinResult']['Paging']['TotalRecords'] > 0 :
            shixin_result = result['ShiXinResult']['Items']
            collection = db.shixin_result
            dbret = collection.insert_many(shixin_result)
            
def dump_Shixin_and_zhixing():
    collection = db.shixin_result
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('shixin_result.xlsx')
    
    collection = db.zhixing_result
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('zhixing_result.xlsx')
    
def remove_data():
    db.Announcement.delete_many({})
    db.JudgmentDoc.delete_many({})
    db.JudgementDetail.delete_many({})
    db.shixin.delete_many({})
    db.zhixing.delete_many({})
    db.zhixing_result.delete_many({})
    db.shixin_result.delete_many({})
    
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
#    company_list = company_list(DATA_FILE)
#    company_list = issuers
#finished 北京市国有资产经营有限责任公司
#start 中国中信有限公司
    ii = 0
    len,w = issuers.shape
    for company in issuers.iloc[:,0]:#[:2,0]:
        ii = ii + 1
        print("%d/%d \t%s"%(ii,len,company))   
        JudgmentDoc(company)
#        SearchZhiXing(company)
 
#        CourtAnnouncement(company)
#        jd_detail = JudgementDetail(jd_doc[0]['Id'])
#        SearchZhiXing(company)

#        Shixin_and_zhixing(company)

        
#    dump_Announcement()
#    dump_JudgementDetail()
#    dump_JudgmentDoc()
#    dump_zhixing()
#    dump_shixin()
#    dump_Shixin_and_zhixing()
