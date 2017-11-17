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
collection = db.shixin
collection_new = db.shixin_new
time_str = time.strftime('%Y-%m-%d|%H:%M',time.localtime(time.time()))
filename = "new_shixin%s.xlsx"%(time_str)

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

import random
#import math
def getCompanyList():
    db_comp = client.company_data
    collection = db_comp.distinct_company_list
    query = collection.find({},{"_id":0,"CODE":0})
    company_df = pd.DataFrame(list(query))
    return company_df
   
def recursiveSearchZhixing(name, PageIndex=1):
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
            for x in range(len(result)):   
                result[x]['_id'] = result[x]['Id']     
#                print(result[x]['_id'])
            dbret = collection.insert_many(result)
            print(dbret)
        if PageIndex < page_cnt:
            ts = random.random()*3 + 1
            print("wait %f \tpages: %d/%d"%(ts,PageIndex,page_cnt))
#            time.sleep(ts)
            recursiveSearchZhixing(name,PageIndex+1)
        else:
            print("finished %s"%name)
            return 
        
def getShixinRecord(name):
    query = collection.find({'Name':name},{'_id':0,'Id':1})
    df = pd.DataFrame(list(query))
    return df
    
def exists_shixin_id(id):
    query = collection.find({'_id':id},{'_id':0,'Id':1})
    flag = (len(list(query)) > 0)
    return flag
    
def updateShixin(name,PageIndex=1):
#    PageIndex = 1
#    name='太原天龙集团股份有限公司'
#    query = collection.find({'Name':name},{}).limit(50)    
    url = BASE_URL + 'CourtV4/SearchShiXin?key=%s&isExactlySame=true&pageSize=50&PageIndex=%d&searchKey='%(ApiKey,PageIndex) + urllib.parse.quote(name)
    resp=urllib.request.urlopen(url).read()
    ret = json.loads(resp)
    if ret['Status'] in err_code:
        print(ret)
        return ret['Status']
    
    if ret['Status'] == '200':
#        print("url:" + url)   
#        print(ret)
#    else:
        PageIndex = ret['Paging']['PageIndex']
        PageSize = ret['Paging']['PageSize']
        TotalRecords = ret['Paging']['TotalRecords']
        if ret['Paging']['TotalRecords'] > 0:
            result = ret['Result']
        
        rec_db = getShixinRecord(name) # read the old records
        
        if len(rec_db) == 0: # new found, insert into new and old
            for x in range(len(result)):
                if exists_shixin_id(result[x]['Id']):
                    continue
                print(result[x])
                result[x]['_id'] = result[x]['Id']
                try:
                    ret_db_new = collection_new.insert_one(result[x])
                    ret_db = collection.replace_one({'_id':result[x]['_id']},result[x],upsert=True)
                except:
                    print('exception')
                    print(new_records)
        else: # there have been some in the db, need to check the ids        
            db_id_set = set(rec_db['Id'])
            df = pd.DataFrame(result)
            df = df[df['Name']==name]  # sometimes bother
            new_id_set = set(df['Id'])
            update_ids = new_id_set - db_id_set
            update_df = pd.DataFrame(list(update_ids),columns=['Id'])
            new_records = df[df['Id'].isin(update_df['Id'])]
#            new_records = df[~df.isin(rec_db)['Id']] # df left out join rec_db
            if len(new_records) > 0:
                print(new_records)
                new_records['_id'] = new_records['Id'].copy()
                insert_record = json.loads(new_records.to_json(orient='records'))
                try:
                    collection_new.insert_many(insert_record)
                    collection.insert_many(insert_record)
                except:
                    print('exception')
                    print(new_records)
                
    return ret['Status']


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
    subject = "新增失信报告"
    
    if my.send_mail(mailto_list,subject, output_dir, attach, content):
        print("报告发送成功")
    else:
        print("报告发送失败")

      
if __name__ == "__main__":

    remove_data()
    company_df = getCompanyList()
    l,w = company_df.shape
    for i in range(l):
        name = company_df.values[i][0]
#        print("%d/%d\t%s"%(i+1,l,name))
        ret = updateShixin(name)
        if ret in err_code:
            break
    if check_update():
        print('something is updated')
        output_dir = os.getcwd()
        send_mail(filename)
    else:
        print('nothing updated')
    
        