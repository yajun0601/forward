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
from urllib import parse
import time
import os
mailto_list=["zhengyajun@zhengqf.com","cuiyuzheng@zhengqf.com"]#,"zqf_btc@126.com"

client = MongoClient("mongodb://localhost:27017/")
db = client.qichacha_new
collection = db.JudgmentDoc
collection_new = db.JudgmentDoc_new
time_str = time.strftime('%Y-%m-%d|%H:%M',time.localtime(time.time()))
filename = "new_JudgmentDoc%s.xlsx"%(time_str)

err_code = ['101','102','103','104','105','107','108','109','110','199']


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

def getCompanyList():
    db_comp = client.company_data
    collection = db_comp.distinct_company_list
    query = collection.find({},{"_id":0,"CODE":0})
    company_df = pd.DataFrame(list(query))
    return company_df

def exists_judgementDoc_id(id):
    query = collection.find({'_id':id},{'_id':0,'Id':1})
    flag = (len(list(query)) > 0)
    return flag
    
#company = '北京京东世纪贸易有限公司'
#pageIndex=1
def update_JudgementDoc(company,PageIndex=1):
    url = BASE_URL + 'JudgeDocV4/SearchJudgmentDoc?isExactlySame=True&pageSize=50&key=%s&searchKey='%(ApiKey) + urllib.parse.quote(company)+"&pageIndex=%d"%(PageIndex)  # Chinese characters need to be quoted
#    print("%s url: \t%s"%(company,url))
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    
    if ret['Status'] in err_code:
        print(ret)
        return ret['Status']        
    
    if ret['Status'] == '200':
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = int(TotalRecords/PageSize) + 1
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            for rr in result:
                if exists_judgementDoc_id(rr['Id']):
                    continue
                rr['_id'] = rr['Id']
                rr['Name'] = company
                print(rr)
                try:
                    collection.insert_one(rr)
                    collection_new.insert_one(rr)
                except:
                    print('insert error: %s'%rr['_id'])

        if PageIndex < page_cnt:
            update_JudgementDoc(company,PageIndex+1)
        else:
            print("finished %s"%company)
            return ret['Status']
        
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

    url = BASE_URL + 'JudgeDocV4/SearchJudgmentDoc?isExactlySame=True&pageSize=50&key=%s&searchKey='%(ApiKey) + urllib.parse.quote(company)+"&pageIndex=%d"%(PageIndex)  # Chinese characters need to be quoted
#    print("%s url: \t%s"%(company,url))
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    
    if ret['Status'] in err_code:
        print(ret)
        return ret['Status']        
    
    if ret['Status'] == '200':
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        page_cnt = int(TotalRecords/PageSize) + 1
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
            for rr in result:
                rr['_id'] = rr['Id']
                rr['Name'] = company
            collection.insert_many(result)
#            print(dbret)
        if PageIndex < page_cnt:
#            ts = random.random()*3 + 1
            print("wait %f \tpages: %d/%d"%(1,PageIndex,page_cnt))
#            time.sleep(ts)
            recursiveJudgmentDoc(company,PageIndex+1)
        else:
            print("finished %s"%company)
            return ret['Status']

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
    
    collection = db.JudgementDetail
    query = collection.find({},{'_id':0,'Id':1}).sort("_id",pymongo.DESCENDING).limit(10)
    record_id = list(query)
    
    ii = 0
    for id in idList: #[:10]:
        ii = ii + 1
        print("%d/%d\t%s"%(ii,length,id['Id']))
        ret = JudgementDetail(id['Id'])
        if ret in ['101','102','103','104','105','107','108','109','110','199']:
            break
        

def dump_JudgementDetail():
    collection = db.JudgementDetail
    records = collection.find({},{'_id':0})
    df = pd.DataFrame(list(records))
    df.to_excel('JudgementDetail.xlsx')

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


def remove_data():
    db.Announcement.delete_many({})
    db.JudgmentDoc.delete_many({})
    db.JudgementDetail.delete_many({})
    db.shixin.delete_many({})
    db.zhixing.delete_many({})
    db.zhixing_result.delete_many({})
    db.shixin_result.delete_many({})

    
def JudgmentDoc_2json():
    client = MongoClient("mongodb://localhost:27017/")
    db = client.qichacha_new
    collection = db.JudgmentDoc_isExactlySame
    query = collection.find().limit(10)
    record = pd.DataFrame(list(query))
    
def remove_data():
    import os
    collection_new.delete_many({})
    if os.path.exists(filename):
        os.remove(filename)
        
def check_update():
    query = collection_new.find()
    df = pd.DataFrame(list(query))
    flag = (len(df)>0)
    if flag:
        df.to_excel(filename)
    # check file exist, to send alert email
    return flag

def send_mail(attach):
    import my_email as my
    output_dir = os.getcwd()
    df = pd.read_excel("%s/%s"%(output_dir,attach))
    namelist = list(set(df['Name']))
    content = '\n'.join(namelist)
    subject = "新增裁判文书"
    
    if my.send_mail(mailto_list,subject, output_dir, attach, content):
        print("报告发送成功")
    else:
        print("报告发送失败")

if __name__ == "__main__":
    remove_data()
    company_df = getCompanyList()
    l,w = company_df.shape
    for i in range(7000,l):
        name = company_df.values[i][0]
        print("%d/%d\t%s"%(i+1,l,name))
        ret = update_JudgementDoc(name)
        if ret in err_code:
            break     
        
    if check_update():
        print('something is updated')
        output_dir = os.getcwd()
        send_mail(filename)
    else:
        print('nothing updated')
    
    