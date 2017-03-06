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


def load(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
        print(data)
        return data
'''
请求示例：http://i.yjapi.com/ECIRelation/SearchInvestment?key=ApiKey&keyword=苏州朗动网络科技有限公司
key		String	是		接口ApiKey
keyword	String	是		查询关键字（公司名称或股东）
pageSize	Integer	否	每页条数，默认10条，最大不超过50条
pageIndex	Integer	否	页码，默认第1页
dtype	String	否	返回数据格式：json或xml，默认json
'''
def SearchInvestment():
#    filename = 'SearchInvestment.json'
    filename = 'JudgmentDoc.json'
#    filename = 'CourtAnnouncement.json'
#    filename = 'JudgementDetail.json'
    info = load(filename)
    print(info)

    return info
'''
请求示例：http://i.yjapi.com/ECIRelation/SearchCompanyRelation?key=ApiKey&keyNo=f625a5b661058ba5082ca508f99ffe1b
请求参数：
key		String	是	接口	ApiKey
keyNo	String	是	公司内部关联主键KeyNo
upstreamCount	Integer	否	子级被投资公司层数（取值范围1-3，默认为1）
downstreamCount	Integer	否	父级投资公司层数（取值范围1-3，默认为1）
dtype	String	否	返回数据格式：json或xml，默认json
'''
def SearchCompanyRelation(keyNo):
	pass

'''
请求示例：http://i.yjapi.com/ECIRelation/SearchRelationship?key=ApiKey&searchKey=陈德强,施阳
key	String	是			接口	ApiKey
searchKey	String	是	根据人名或者公司全称查询关联关系，使用空格或者英文逗号(,)作分割符，接受1组以上和10组以下参数
dtype	String	否		返回数据格式：json或xml，默认json
'''
def SearchRelationship():
	pass

'''
请求示例：http://i.yjapi.com/ECIRelation/SearchTreeRelationMap?key=ApiKey&keyNo=f625a5b661058ba5082ca508f99ffe1b
key		String	是			接口	ApiKey
keyNo	String	是			公司内部关联主键KeyNo
upstreamCount	Integer	否	子级被投资公司层数（取值范围1-3，默认为1）
downstreamCount	Integer	否	父级投资公司层数（取值范围1-3，默认为1）
dtype	String	否			返回数据格式：json或xml，默认json
'''
def SearchTreeRelationMap():
    pass



if __name__ == "__main__":
    rec = SearchInvestment()
    if rec['Status'] == '200':
        record = rec['Result']
    else:
        print(record)
    
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
        
    
    
