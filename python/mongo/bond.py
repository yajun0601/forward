# -*- coding: utf-8 -*-
"""
Created on Sat Apr  1 17:02:07 2017

@author: Administrator
"""

from WindPy import *
import pandas as pd
from numpy  import *
from pymongo import *
import json


w.start()
client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.stocks
#ret = w.wset("sectorconstituent","date=2017-04-01;sectorid=1000004559000000")

def state():
    #获取所有国有企业信用债代码
    stateOwned = w.wset('sectorconstituent','date=2017-04-01;sectorid=1000004554000000;field=wind_code')
    # returned 23200
    comp_name_list = list()
    for i in range(1,10):
        print(i*2320,(i+1)*2320-1)
        comp_name = w.wss(stateOwned.Data[0][2320*i:(i+1)*2320-1],'comp_name')
        df = pd.DataFrame(comp_name.Data[0], index = stateOwned.Data[0][2320*i:(i+1)*2320-1], columns=["comp_name"])
        df["_id"] = stateOwned.Data[0][2320*i:(i+1)*2320-1]
        df["type"] = "国有企业"
        insert_record = json.loads(df.to_json(orient='records'))
        ret = db.bond.insert_many(insert_record)
        print(ret)
        
        #comp_name_list.append(comp_name.Data[0])
    
    
def private():    
    #获取所有民营企业信用债代码
    privateOwned = w.wset('sectorconstituent','date=2017-04-01;sectorid=1000004555000000;field=wind_code')
    comp_name = w.wss(privateOwned.Data[0],'comp_name')
    
    df = pd.DataFrame(comp_name.Data[0], index = privateOwned.Data[0], columns=["comp_name"])
    df["_id"] = privateOwned.Data[0]
    df["type"] = "民营企业"
    insert_record = json.loads(df.to_json(orient='records'))
    ret = db.bond.insert_many(insert_record)
    print(len(privateOwned.Data[0]))
    
def foreign():
    #获取所有外资企业信用债代码
    foreignOwned = w.wset('sectorconstituent','date=2017-04-01;sectorid=1000004556000000;field=wind_code')
    comp_name = w.wss(foreignOwned.Data[0],'comp_name')
    
    df = pd.DataFrame(comp_name.Data[0], index = foreignOwned.Data[0], columns=["comp_name"])
    df["_id"] = foreignOwned.Data[0]
    df["type"] = "外资企业"
    insert_record = json.loads(df.to_json(orient='records'))
    ret = db.bond.insert_many(insert_record)
    print(ret)
    
def collective():
    #获取所有集体企业信用债代码
    otherOwned = w.wset('sectorconstituent','date=2017-04-01;sectorid=1000004557000000;field=wind_code')
    comp_name = w.wss(otherOwned.Data[0],'comp_name')
    
    df = pd.DataFrame(comp_name.Data[0], index = otherOwned.Data[0], columns=["comp_name"])
    df["_id"] = otherOwned.Data[0]
    df["type"] = "集体企业"
    insert_record = json.loads(df.to_json(orient='records'))
    ret = db.bond.insert_many(insert_record)
    print(ret)
    
def public():
    #获取所有公众企业信用债代码
    otherOwned = w.wset('sectorconstituent','date=2017-04-01;sectorid=1000004558000000;field=wind_code')
    comp_name = w.wss(otherOwned.Data[0],'comp_name')
    
    df = pd.DataFrame(comp_name.Data[0], index = otherOwned.Data[0], columns=["comp_name"])
    df["_id"] = otherOwned.Data[0]
    df["type"] = "公众企业"
    insert_record = json.loads(df.to_json(orient='records'))
    ret = db.bond.insert_many(insert_record)
    print(ret)
    
def other():
    #获取所有其他企业信用债代码
    otherOwned = w.wset('sectorconstituent','date=2017-04-01;sectorid=1000004559000000;field=wind_code')
    comp_name = w.wss(otherOwned.Data[0],'comp_name')
    
    df = pd.DataFrame(comp_name.Data[0], index = otherOwned.Data[0], columns=["comp_name"])
    df["_id"] = otherOwned.Data[0]
    df["type"] = "其他企业"
    insert_record = json.loads(df.to_json(orient='records'))
    ret = db.bond.insert_many(insert_record)
    print(ret)
    
if __name__ == "__main__":
#    cashflow_sheet()
    state()
    private()
    public()
    collective()
    foreign()

#    client.close()            