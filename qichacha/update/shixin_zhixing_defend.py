# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 17:11:33 2017

@author: yajun
"""

import json
import pandas as pd
from datetime import datetime,timedelta
from pymongo import MongoClient
from bson.son import SON
import my_db

client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.qichacha_new

#360 days before  till now
#collection = db.JudgementDetail

def shixin_count():
    # 失信总次数
    collection = db.shixin
    #db.collection.aggregate([$group:{_id:'$Name', num:{$sum:1}}])
    pipeline = [{"$group" : {"_id" : "$Name", "num" : {"$sum":1}}}]
    code = collection.aggregate(pipeline)
    record = pd.DataFrame(list(code)) #Convert the input to an array.
    if len(record) != 0:
        record.columns = ['name', 'shixin']
    return record

def zhixing_count():
    # 执行总次数
    collection = db.zhixing
    #db.collection.aggregate([$group:{_id:'$Name', num:{$sum:1}}])
    pipeline = [{"$group" : {"_id" : "$Name", "num" : {"$sum":1}}}]
    code = collection.aggregate(pipeline)
    record = pd.DataFrame(list(code)) #Convert the input to an array.
    if len(record) != 0:
        record.columns = ['name', 'zhixing']
    return record

def defendant_count():
    # 被告总次数
#    db = client.companies
    collection = db.JudgementDetail
    pipeline = [
    { "$project": {"Defendantlist":1}},
    { "$unwind": "$Defendantlist"},
    { "$group": {"_id":"$Defendantlist", "cnt":{"$sum":1}}},
    {"$sort": SON([("cnt", -1)])}
    ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', 'defendant']
    # 与发债主体 merge
    
    return record

def appellor_count():
    # 原告总次数
#    db = client.companies
    collection = db.JudgementDetail
    pipeline = [
    { "$project": {"Appellor":1}},
    { "$unwind": "$Appellor"},
    { "$group": {"_id":"$Appellor", "cnt":{"$sum":1}}},
    {"$sort": SON([("cnt", -1)])}
    ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', 'appellor']
    # 与发债主体 merge
    
    return record

def total_judgement_count():
    defendant_df = defendant_count()
    appellor_df = appellor_count()
    total_df = defendant_df.merge(appellor_df,on='name', how='outer')
    total_df.fillna(0)
    total_df
    total_df = total_df.fillna(0)
    total_df['judgement']=total_df['defendant'] + total_df['appellor']
    return total_df

def sued_by_bank():
#    db = client.companies
    collection = db.JudgementDetail   
    pipeline = [
    { "$project": {"Defendantlist":1,"Appellor":1}},
    { "$unwind": "$Appellor"},
    { "$unwind": "$Defendantlist"},
    { "$match":{"Appellor":{"$regex":"银行"}}},
    { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
    {"$sort": SON([("cnt", -1)])}
    ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', 'bank']
    return record

def sued_by_small_loan_cmpny():
#    db = client.companies
    collection = db.JudgementDetail   
    pipeline = [
    { "$project": {"Defendantlist":1,"Appellor":1}},
    { "$unwind": "$Appellor"},
    { "$unwind": "$Defendantlist"},
    { "$match":{"Appellor":{"$regex":"贷款"}}},
    { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
    {"$sort": SON([("cnt", -1)])}
    ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', 'small_loan_cmpny']
    return record

def sued_in_arrears():
#    db = client.companies
    collection = db.JudgementDetail   
    pipeline = [
    { "$project": {"Defendantlist":1,"Content":1}},
    { "$unwind": "$Defendantlist"},
    { "$match":{"Content":{"$regex":"欠款"}}},
    { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
    {"$sort": SON([("cnt", -1)])}
    ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', 'in_arrears']
    return record


#defendant_df = defendant_count()
#appellor_df = appellor_count()
total_judgement_df = total_judgement_count()
bank_df = sued_by_bank()
small_loan_df = sued_by_small_loan_cmpny()
in_arrears_df = sued_in_arrears()
shixin_df = shixin_count()
zhixing_df = zhixing_count()


#result = defendant_df.merge(appellor_df, on='_id', how='outer')
result = total_judgement_df.merge(bank_df, on='name', how='outer')
result = result.merge(small_loan_df, on='name', how='outer')
result = result.merge(in_arrears_df, on='name', how='outer')
result = result.merge(shixin_df, on='name', how='outer')
result = result.merge(zhixing_df, on='name', how='outer')
#result = result.fillna(0)

def pingan_trust():
    issuers = pd.read_excel('平安信托-SW综合-公司列表.xlsx',sheetname=[0], header = 0)[0]
    issuers = my_db.getCompanyList()
    issuers.columns= ['name']
    focus = issuers.merge(result, on='name', how='left')
    
    focus = focus.sort_values('shixin',axis=0,ascending=False)
    import time
    time_str = time.strftime('%Y%m%d',time.localtime(time.time()))
    focus['rptDate']=time_str
    def insert_into_db(df):
        client = MongoClient("mongodb://192.168.10.60:27017/")
        db = client.qichacha_new
        if len(df) !=0 :
            insert_record = json.loads(df.to_json(orient='records'))
            ret = db.shixin_zhixing_defend.insert_many(insert_record)
        
    insert_into_db(focus)     
         
    report = focus.dropna(axis=0, how='any',thresh=3)
    report.to_excel("shixin_zhixing_bank.xlsx")
#db = client.companies
#collection = db.total_nums
#insert_record = json.loads(result.to_json(orient='records'))
#ret = db.total_nums.insert_many(insert_record)
# 写数据库
#collection = db.
#df = sued_in_arrears()
#insert_record = json.loads(df.to_json(orient='records'))
#ret = db.collection.insert_many(insert_record)
if __name__ == "__main__":
    
    
