#!/bin/python
#coding=utf-8

import numpy as np
import pandas as pd
import sys

#from pymongo import *
import json
from datetime import datetime,timedelta
from pymongo import MongoClient
import pymongo
import urllib.request

client = MongoClient("mongodb://localhost:27017/")
db = client.qichacha_new

DEV = 0
if DEV == 1 :
    DATA_FILE='data/bond_info.xlsx'
    ApiKey='ApiKey'
    BASE_URL='http://dev.i.yjapi.com/'
else:
    ApiKey = 'f104ea4923df4c58b75f3b37f8c47329'
    BASE_URL='http://i.yjapi.com/'
    
def load(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
        print(data)
        return data

import random
#import math
def getCompanyList():
    db_comp = client.company_data
    collection = db_comp.distinct_company_list
    query = collection.find({},{"_id":0,"CODE":0})
    company_df = pd.DataFrame(list(query))
    return company_df

def recursiveJudgmentDoc(company, PageIndex=1):
    '''
    请求示例：http://i.yjapi.com/JudgeDocV4/SearchJudgmentDoc?key=ApiKey&searchKey=北京京东世纪贸易有限公司
    请求参数：
    searchKey   String  是   查询关键字
    isExactlySame   String  否   名称是否需要与关键字一样,True或者False(Default value is False)
    caseType    String  否   案件类型(ms:民事 xs:刑事 xz:行政 zscq:知识产权 pc:赔偿 zx:执行)默认为空
    pageSize    int     否   每页条数，默认10条，最大不超过50条
    pageIndex   int     否   页码，默认第1页
    key         String  是   接口ApiKey
    dtype       String  否   返回数据格式：json或xml，默认json
    
    filename = 'JudgmentDoc.json'
    ret = load(filename)
    '''
#    DEV_URL='http://dev.i.yjapi.com/Judicial/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey)
#    BASE_URL='http://i.yjapi.com/JudgeDocV4/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey)
    collection = db.JudgmentDoc_isExactlySame
    url = BASE_URL + 'JudgeDocV4/SearchJudgmentDoc?isExactlySame=True&pageSize=50&key=%s&searchKey='%(ApiKey) + urllib.parse.quote(company)+"&pageIndex=%d"%(PageIndex)  # Chinese characters need to be quoted
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
        page_cnt = int(TotalRecords/PageSize) + 1
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()*3 + 1
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            recursiveJudgmentDoc(company,PageIndex+1)
        else:
            print("finished %s"%company)
            return
        

def getLatestAnnouncement(companyName):
    # get latest Announcement from internet, if new information got, update database
    # 1. select the latest record for the companyName
    companyName = '平安银行股份有限公司'
    collection = db.Announcement
    query = collection.find({"CompanyName" : companyName},{"_id":0,"PublishedDate":1,"Content":1}).sort("PublishedDate",pymongo.DESCENDING).limit(5)
    res = list(query)
    latestPublishedDate = res[4]['PublishedDate']
    # query with the latest PublishedDate, maybe get multiple records
    query = collection.find({"CompanyName" : companyName,"PublishedDate":latestPublishedDate},{"_id":0,"PublishedDate":1,"Content":1})
    record = pd.DataFrame(list(query))
    
    # request lates record from internet
    url = BASE_URL+'CourtNoticeV4/SearchCourtAnnouncement?key=%s&PageIndex=%d&pageSize=50&companyName='%(ApiKey,1)
    url = url + urllib.parse.quote(companyName) # Chinese characters need to be quoted
    print(url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] != '200':
        print(ret)
    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = int(TotalRecords/PageSize) + 1
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            response = pd.DataFrame(result)
            newer_df = response[response['PublishedDate'] > latestPublishedDate]

            
#            2017-10-24T00:00:00+08:00   2017-10-24T00:00:00+08:00
#...
     
def recursiveAnnouncement(companyName,PageIndex=1):
    '''
    请求示例：http://i.yjapi.com/CourtNoticeV4/SearchCourtAnnouncement?key=ApiKey&companyName=北京京东世纪贸易有限公司
    companyName String  是   公司名字
    pageSize    int     否   每页条数，默认10条，最大不超过50条
    pageIndex   int     否   页码，默认第1页
    key         String  是   接口ApiKey
    dtype       String  否   返回数据格式：json或xml，默认json
    '''
#    companyName = '深圳市全新好股份有限公司'
    collection = db.Announcement
    url = BASE_URL+'CourtNoticeV4/SearchCourtAnnouncement?key=%s&PageIndex=%d&pageSize=50&companyName='%(ApiKey,PageIndex)
    url = url + urllib.parse.quote(companyName) # Chinese characters need to be quoted
    print(url)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] != '200':
        print(ret)
    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = int(TotalRecords/PageSize) + 1
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            for rr in result:
                rr['CompanyName']=companyName
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()*3 + 1
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            recursiveAnnouncement(companyName,PageIndex+1)
        else:
            print("finished %s"%companyName)
            return 


def CourtAnnouncement(companyName,pageSize=50):
    '''
    请求示例：http://i.yjapi.com/CourtNoticeV4/SearchCourtAnnouncement?key=ApiKey&companyName=北京京东世纪贸易有限公司
    companyName String  是   公司名字
    pageSize    int     否   每页条数，默认10条，最大不超过50条
    pageIndex   int     否   页码，默认第1页
    key         String  是   接口ApiKey
    dtype       String  否   返回数据格式：json或xml，默认json
    '''
    companyName = '北京京东世纪贸易有限公司'
    collection = db.Announcement
    url = BASE_URL+'CourtNoticeV4/SearchCourtAnnouncement?key=%s&pageSize=50&companyName='%(ApiKey)
    url = url + urllib.parse.quote(companyName) # Chinese characters need to be quoted
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
#    print(url)
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
#            print('insert detail:' + detail['_id'])
            jd_collection.insert_one(detail)
        # TODO: if the record need to be updated?
        return detail
    return ret

def getJudgementDetail():
    jd_collection = db.JudgmentDoc_isExactlySame
    query = jd_collection.find({},{'_id':0,'Id':1}).sort("Id",pymongo.ASCENDING)
    idList = list(query)
    length = len(idList)
    ii = 0
    for id in idList: #[:10]:
        ii = ii + 1
        print("%d/%d\t%s"%(ii,length,id['Id']))
        JudgementDetail(id['Id'])
        

def dump_JudgementDetail():
    collection = db.JudgementDetail
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('JudgementDetail.xlsx')
from urllib import parse

def JudgmentDoc(company):
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

    url = BASE_URL + 'Judicial/SearchJudgmentDoc?key=%s&searchKey='%(ApiKey) + urllib.parse.quote(company) # Chinese characters need to be quoted
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

def SearchZhixing(name, PageIndex=1):
    '''
    接口地址：http://i.yjapi.com/Court/SearchShiXin
    支持格式：JSON/XML
    请求方式：GET
    请求示例：http://i.yjApi.com/CourtV4/SearchZhiXing?key=ApiKey&searchKey=张三 '''
#    name='河南亚华安全玻璃有限公司'
    collection = db.zhixing
    url = BASE_URL + 'CourtV4/SearchZhiXing?key=%s&isExactlySame=true&pageSize=50&PageIndex=%d&searchKey='%(ApiKey,PageIndex) + urllib.parse.quote(name)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] != '200':
        print("url:" + url)        
        print(ret)
    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = int(TotalRecords/PageSize) + 1
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()*3 + 1
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            SearchZhixing(name,PageIndex+1)
        else:
            print("finished %s"%name)
            return 
        

def dump_zhixing():
    collection = db.zhixing
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('zhixing.xlsx')

    
def SearchShiXin(name, PageIndex=1):
    '''
    接口地址：http://i.yjapi.com/Court/SearchShiXin
    支持格式：JSON/XML
    请求方式：GET
    请求示例：http://i.yjApi.com/Court/SearchShiXin?key=ApiKey&searchKey=张三 '''
#    name='河南亚华安全玻璃有限公司'
    collection = db.shixin
    url = BASE_URL + 'CourtV4/SearchShiXin?key=%s&isExactlySame=true&pageSize=50&PageIndex=%d&searchKey='%(ApiKey,PageIndex) + urllib.parse.quote(name)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] != '200':
        print("url:" + url)        
        print(ret)
    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = int(TotalRecords/PageSize) + 1
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()*3 + 1
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            SearchShiXin(name,PageIndex+1)
        else:
            print("finished %s"%name)
            return 
        
        
def dump_shixin():
    collection = db.shixin
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('shixin.xlsx')
    

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
#    getJudgementDetail()

    company_df = getCompanyList()
    l,w = company_df.shape
    for i in range(l):
        name = company_df.values[i][0]
        print("%d/%d\t%s"%(i+1,l,name))
        SearchShiXin(name)
#        recursiveAnnouncement(name)
#        recursiveJudgmentDoc(name)
        
        
        
        
## get company list
##    company_list = company_list(DATA_FILE)
#    company_list = issuers
#    for company in issuers.iloc[:2,0]:
#        print(company)    
#        CourtAnnouncement(company)
##        jd_detail = JudgementDetail(jd_doc[0]['Id'])
#        SearchZhiXing(company)
#        SearchShiXin(company)
#        Shixin_and_zhixing(company)
#        jd_doc = JudgmentDoc(company)
#        
#    dump_Announcement()
#    dump_JudgementDetail()
#    dump_JudgmentDoc()
#    dump_zhixing()
#    dump_shixin()
#    dump_Shixin_and_zhixing()
