#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 15:51:54 2017

@author: yajun
"""

import pandas as pd
import json
import os,time,sys,re

from pymongo import *

client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.companies
jd_collection = db.JudgementDetail

#FILE='data/t_judge_detail_json.txt'
FILE='data/深圳正前方数据_裁判文书20170508.txt'
def load(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
        print(data)
        return data
    
def load_detail():    
    for line in open(FILE):  
        print(line)
        index = line.find('\t')
        resp = line[index:]
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
    
if __name__ == "__main__":
    print('parsing ... ')
    ii = 0
    found = 0
    insert = 0
    total = 0
    for line in open(FILE):  
#        print(line)
        total += 1
        index = line.find('\t')
        resp = line[index:]
        ret = json.loads(resp)
#        print(ret)
        if ret['Status'] == 200:
            ii = ii+1
            detail = ret['Result']
            detail['_id'] = detail['KeyNo']
            print(ii,detail['_id'],detail['CaseName'])
            
            in_collection = jd_collection.find_one({'_id':detail['_id']})
            if in_collection is None:   # only if the id does not exist
                insert += 1
                print(' '+ str(insert) +' insert detail:' + detail['_id'])
#                jd_collection.insert_one(detail)
            else:
                found += 1
                print(' ' + str(found) + 'found: ' + detail['_id'])
            
            # TODO: if the record need to be updated?
            #return detail
        else:
            print(ret)
            
    print("insert: %d, found: %d, total: %d"%(insert,found,total))
            #return None
    