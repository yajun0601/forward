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
import time
#client = MongoClient("mongodb://192.168.10.60:27017/")
client = MongoClient("mongodb://1t611714m7.iask.in:22839")
db = client.qichacha_new

#360 days before  till now
#collection = db.JudgementDetail
START_DATE = '2015-01-01'
#END_DATE = '2016-12-31'
END_DATE = time.strftime('%Y-%m-%d',time.localtime(time.time()))


def shixin_count(start_date=START_DATE,end_date=END_DATE,name=None):
    #失信总次数
    '''    db.shixin.aggregate([
    { "$match":{"Publicdate":{"$gt":"2017-09-15"}}},
    { "$match":{"Publicdate":{"$lt":"2017-09-25"}}},
    {"$group":{"_id":{"name":'$Name'}, "count":{"$sum":1}}},
    {"$sort":{"count":-1}}
    ]) '''
    collection = db.shixin
    #db.collection.aggregate([$group:{_id:'$Name', num:{$sum:1}}])
    pipeline = [
    { "$match":{"Publicdate":{"$gte":start_date}}},
    { "$match":{"Publicdate":{"$lt":end_date}}},
    {"$group":{"_id":'$Name', "count":{"$sum":1}}},
    {"$sort":{"count":-1}}
    ]
    code = collection.aggregate(pipeline)
    record = pd.DataFrame(list(code)) #Convert the input to an array.
    if len(record) != 0:
        record.columns = ['name', '失信']
    return record

def zhixing_count(start_date=START_DATE,end_date=END_DATE,name=None):
    # 执行总次数
    collection = db.zhixing
    #db.collection.aggregate([$group:{_id:'$Name', num:{$sum:1}}])
    if name == None:
        pipeline = [
        { "$match":{"Liandate":{"$gte":start_date}}},
        { "$match":{"Liandate":{"$lt":end_date}}},
        {"$group":{"_id":'$Name', "count":{"$sum":1}}},
        {"$sort":{"count":-1}}
        ]  
    else:
        pipeline = [
        { "$match":{"Name":name}},
        { "$match":{"Liandate":{"$gte":start_date}}},
        { "$match":{"Liandate":{"$lt":end_date}}},
        {"$group":{"_id":'$Name', "count":{"$sum":1}}},
        {"$sort":{"count":-1}}
        ] 
    code = collection.aggregate(pipeline)
    record = pd.DataFrame(list(code)) #Convert the input to an array.
    if len(record) != 0:
        record.columns = ['name', '执行']
    return record

def defendant_count(start_date=START_DATE,end_date=END_DATE,name=None):
    # 被告总次数
#    db = client.companies
    collection = db.JudgmentDoc_isExactlySame_Clean
    if name == None:
        pipeline = [
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1}},
        { "$unwind": "$Defendantlist"},
        { "$group": {"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]
    else:
        pipeline = [
        { "$match":{"Name":name}},
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1}},
        { "$unwind": "$Defendantlist"},
        { "$group": {"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]    
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', '被告']
    # 与发债主体 merge
    
    return record

def appellor_count(start_date=START_DATE,end_date=END_DATE,name=None):
    # 原告总次数
#    db = client.companies
    collection = db.JudgmentDoc_isExactlySame_Clean
    if name == None:
        pipeline = [
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Prosecutorlist":1}},
        { "$unwind": "$Prosecutorlist"},
        { "$group": {"_id":"$Prosecutorlist", "cnt":{"$sum":1}}},
    #    {"$match":{'Prosecutorlist':'无锡产业发展集团有限公司'}},
        {"$sort": SON([("cnt", -1)])}
        ]
    else:
        pipeline = [
        { "$match":{"Name":name}},       
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Prosecutorlist":1}},
        { "$unwind": "$Prosecutorlist"},
        { "$group": {"_id":"$Prosecutorlist", "cnt":{"$sum":1}}},
    #    {"$match":{'Prosecutorlist':'无锡产业发展集团有限公司'}},
        {"$sort": SON([("cnt", -1)])}
        ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', '原告']
    # 与发债主体 merge
    
    return record

def total_judgement_count(start_date=START_DATE,end_date=END_DATE,name=None):
    
    defendant_df = defendant_count(start_date,end_date)
    appellor_df = appellor_count(start_date,end_date)
#    if len(defendant_df) > 0 and len(appellor_df) > 0:
    total_df = defendant_df.merge(appellor_df,on='name', how='outer')
    total_df.fillna(0)
#    total_df
    total_df = total_df.fillna(0)
    total_df['诉讼']=total_df['被告'] + total_df['原告']
    return total_df

def sued_by_bank(start_date=START_DATE,end_date=END_DATE,name=None):
    collection = db.JudgmentDoc_isExactlySame_Clean
    if name == None:
        pipeline = [
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1,"Prosecutorlist":1}},
        { "$unwind": "$Prosecutorlist"},
        { "$unwind": "$Defendantlist"},
        { "$match":{"Prosecutorlist":{"$regex":"银行"}}},
        { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]
    else:
        pipeline = [
        { "$match":{"Name":name}},
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1,"Prosecutorlist":1}},
        { "$unwind": "$Prosecutorlist"},
        { "$unwind": "$Defendantlist"},
        { "$match":{"Prosecutorlist":{"$regex":"银行"}}},
        { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', '银行']
    else:
        record = pd.DataFrame(data=[['',0]],columns=['name', '银行'])
    return record

def sued_by_small_loan_cmpny(start_date=START_DATE,end_date=END_DATE,name=None):
#    db = client.companies
    collection = db.JudgmentDoc_isExactlySame_Clean  
    if name == None:
        pipeline = [
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1,"Prosecutorlist":1}},
        { "$unwind": "$Prosecutorlist"},
        { "$unwind": "$Defendantlist"},
        { "$match":{"Prosecutorlist":{"$regex":"贷款"}}},
        { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]
    else:
        pipeline = [
        { "$match":{"Name":name}},        
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1,"Prosecutorlist":1}},
        { "$unwind": "$Prosecutorlist"},
        { "$unwind": "$Defendantlist"},
        { "$match":{"Prosecutorlist":{"$regex":"贷款"}}},
        { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', '小贷']
    else:
        record = pd.DataFrame(data=[['',0]],columns=['name', '小贷'])
    return record

def sued_in_arrears(start_date=START_DATE,end_date=END_DATE,name=None):
#    db = client.companies
    collection = db.JudgmentDoc_isExactlySame_Clean
    if name == None:
        pipeline = [
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1,"CaseName":1}},
        { "$unwind": "$Defendantlist"},
        { "$match":{"CaseName":{"$regex":"欠款"}}},
        { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]        
    else:
        pipeline = [
        { "$match":{"Name":name}},
        { "$match":{"SubmitDate":{"$gte":start_date}}},
        { "$match":{"SubmitDate":{"$lt":end_date}}},
        { "$project": {"Defendantlist":1,"CaseName":1}},
        { "$unwind": "$Defendantlist"},
        { "$match":{"CaseName":{"$regex":"欠款"}}},
        { "$group":{"_id":"$Defendantlist", "cnt":{"$sum":1}}},
        {"$sort": SON([("cnt", -1)])}
        ]
    ret = collection.aggregate(pipeline)
    record = pd.DataFrame(list(ret))
    if len(record) != 0:
        record.columns = ['name', '欠款']
    else:
        record = pd.DataFrame(data=[['',0]],columns=['name', '欠款'])
    return record


#defendant_df = defendant_count()
#appellor_df = appellor_count()
def get_total(start_date=START_DATE,end_date=END_DATE):
    print(start_date,end_date)
    total_judgement_df = total_judgement_count(start_date,end_date)
    bank_df = sued_by_bank(start_date,end_date)
    small_loan_df = sued_by_small_loan_cmpny(start_date,end_date)
    in_arrears_df = sued_in_arrears(start_date,end_date)
    shixin_df = shixin_count(start_date,end_date)
    zhixing_df = zhixing_count(start_date,end_date)
    
    
    #result = defendant_df.merge(appellor_df, on='_id', how='outer')
    result = pd.DataFrame()
    if len(shixin_df) > 0 and len(zhixing_df) > 0:
        result = shixin_df.merge(zhixing_df, on='name', how='outer')
        if len(result) > 0 and len(total_judgement_df) > 0:
            result = result.merge(total_judgement_df, on='name', how='outer')
            if len(bank_df) > 0:
                result = result.merge(bank_df, on='name', how='outer')
            if len(small_loan_df) > 0:
                result = result.merge(small_loan_df, on='name', how='outer')
            if len(in_arrears_df) > 0:
                result = result.merge(in_arrears_df, on='name', how='outer')
    result = result.fillna(0)
    return result

def insert_into_db(df):
    client = MongoClient("mongodb://192.168.10.60:27017/")
    db = client.qichacha_new
    if len(df) !=0 :
        insert_record = json.loads(df.to_json(orient='records'))
        ret = db.shixin_zhixing_defend.insert_many(insert_record)
    
def pingan_trust():
    result = get_total(start_date=START_DATE,end_date='2016-05-10')
#    issuers = pd.read_excel('平安信托-SW综合-公司列表.xlsx',sheetname=[0], header = 0)[0]
#    issuers = my_db.getCompanyList()
    issuers = pd.read_excel('../peace/平安信托研究.xlsx',sheetname=[0], header = 0)[0]
    issuers.columns= ['name']
    focus = issuers.merge(result, on='name', how='left')
    
    focus = focus.sort_values('失信',axis=0,ascending=False)
    import time
    time_str = time.strftime('%Y%m%d',time.localtime(time.time()))
    focus['rptDate']=time_str

    insert_into_db(focus)     
         
    report = focus.dropna(axis=0, how='any',thresh=3)
    report.to_excel("shixin_zhixing_bank.xlsx")
    
def pingan_trust16():
    from datetime import datetime
    from datetime import timedelta
    collection = db.pingan_total
    
    start = datetime(2016,1,1)
    end = datetime(2017,11,14)
    delta = end - start
    
    aDay = timedelta(days=+1)    
#    issuers = my_db.getCompanyList()
    issuers = pd.read_excel('../peace/平安信托研究.xlsx',sheetname=[0], header = 0)[0]
    for x in range(delta.days) :
        start = start + aDay
        end_date = (start.strftime('%Y-%m-%d'))
#        print(end_date)
        result = get_total(start_date=START_DATE,end_date=end_date)
        if len(result) == 0 :
            continue

#        issuers.columns = ['name']
#        print(issuers)
#        print(result)
        focus = issuers.merge(result, on='name', how='left')
        focus = focus.sort_values('失信',axis=0,ascending=False)
        focus = focus.fillna(0)
        focus['rptDate']=end_date
        
        insert_record = json.loads(focus.to_json(orient='records'))
        collection.insert_many(insert_record)
from datetime import datetime
from datetime import timedelta             
def time_searies_all():

    db = client.qichacha_new
    collection = db.time_searies_all
    
    start = datetime(2015,1,1)
    start_date = (start.strftime('%Y-%m-%d'))
    end = datetime(2016,12,31)
    delta = end - start
    
    aDay = timedelta(days=+1)
    issuers = my_db.getCompanyList()
    issuers = issuers.rename(columns={"COMP_NAME":"name"})
#    issuers = pd.read_excel('../peace/平安信托研究.xlsx',sheetname=[0], header = 0)[0]
    
    for x in range(delta.days) :
        start = start + aDay
        end_date = (start.strftime('%Y-%m-%d'))
        
#        print(end_date)
        result = get_total(start_date=start_date,end_date=end_date)
        if len(result) == 0 :
            continue

#        issuers.columns = ['name']
#        print(issuers)
#        print(result)
        
        focus = issuers.merge(result, on='name', how='left')
#        focus = focus.sort_values('失信',axis=0,ascending=False)
        focus = focus.fillna(0)
        focus['rptDate']=end_date
        
        insert_record = json.loads(focus.to_json(orient='records'))
        collection.insert_many(insert_record)          
#    report = focus.dropna(axis=0, how='any',thresh=3)
#    focus.to_excel("shixin_zhixing_bank_16.xlsx")
def sued_between_holder():
    query = db.holder_name.find({},{"_id":0,"COMP_NAME":1,"HOLDER_NAME":1})
    ret = pd.DataFrame(list(query))
    length = len(ret)
    for x in range(length):
#        print(ret.iloc[x]['COMP_NAME'],ret.iloc[x]['HOLDER_NAME'])
        query = db.JudgmentDoc_isExactlySame_Clean.aggregate([
        { "$match":{"SubmitDate":{"$gt":"2016-01-01"}}},
        { "$match":{"SubmitDate":{"$lt":"2017-11-25"}}},
            { "$project": {"Defendantlist":1,"Prosecutorlist":1}},
            { "$unwind": "$Defendantlist"},
            {"$unwind":"$Prosecutorlist"},
            {"$match":{"Prosecutorlist": ret.iloc[x]['COMP_NAME']}},
            {"$match":{"Defendantlist":ret.iloc[x]['HOLDER_NAME']}}
            ])
        count = len(list(query))
        query = db.JudgmentDoc_isExactlySame_Clean.aggregate([
            { "$match":{"SubmitDate":{"$gt":"2016-01-01"}}},
            { "$match":{"SubmitDate":{"$lt":"2017-11-25"}}},
                { "$project": {"Defendantlist":1,"Prosecutorlist":1}},
                { "$unwind": "$Defendantlist"},
                {"$unwind":"$Prosecutorlist"},
                {"$match":{"Prosecutorlist": ret.iloc[x]['HOLDER_NAME']}},
                {"$match":{"Defendantlist":ret.iloc[x]['COMP_NAME']}}
                ])
        count = count + len(list(query))
        if count > 0:
            print(count,ret.iloc[x]['COMP_NAME'],ret.iloc[x]['HOLDER_NAME'])    


def export_pingan_trust16():
    issuers = pd.read_excel('../peace/平安信托研究.xlsx',sheetname=[0], header = 0)[0]
    writer = pd.ExcelWriter('time_searies_all.xlsx')
    for company in issuers['name']:
        print(company)
        query = db.pingan_total.find({"name":company},{'_id':0,'name':0}).sort("rptDate" , 1)
        data = pd.DataFrame(list(query))
        data = data[ ['执行','失信','原告','被告','诉讼', '小贷', '欠款','银行', 'rptDate']]
        data = data.rename(columns={"银行":"被银行起诉次数","诉讼":"诉讼总数","小贷":"被小贷起诉","欠款":"因欠款被告次数"})        
        data.to_excel(writer, sheet_name=company)
        
    writer.save()

def gen_default_samples(start_date,end_date,name):
    print(start_date,end_date,name)
    total_judgement_df = total_judgement_count(start_date,end_date,name)
    bank_df = sued_by_bank(start_date,end_date,name)
    small_loan_df = sued_by_small_loan_cmpny(start_date,end_date,name)
    in_arrears_df = sued_in_arrears(start_date,end_date,name)
    shixin_df = shixin_count(start_date,end_date,name)
    zhixing_df = zhixing_count(start_date,end_date,name)
    
    
    #result = defendant_df.merge(appellor_df, on='_id', how='outer')
    result = pd.DataFrame()
    if len(shixin_df) > 0 and len(zhixing_df) > 0:
        result = shixin_df.merge(zhixing_df, on='name', how='outer')
        if len(result) > 0 and len(total_judgement_df) > 0:
            result = result.merge(total_judgement_df, on='name', how='outer')
            if len(bank_df) > 0:
                result = result.merge(bank_df, on='name', how='outer')
            if len(small_loan_df) > 0:
                result = result.merge(small_loan_df, on='name', how='outer')
            if len(in_arrears_df) > 0:
                result = result.merge(in_arrears_df, on='name', how='outer')
    result = result.fillna(0)
    return result
    
def get_defaults():
    db_default = client.bonds
    pipeline=[{"$group" : {"_id" : "$发行人", "date" : {"$last":"$发生日期"}}}]
    query = db_default.default.aggregate(pipeline)
    default_cmp = pd.DataFrame(list(query))
    default_cmp['date'] = pd.to_datetime(default_cmp['date']*1000* 1000)  # YY-MM-DD
    default_cmp['df'] = 1
    default_cmp = default_cmp.rename(columns={"_id":"name"})          
    issuers = my_db.getCompanyList()
    issuers = issuers.rename(columns={"COMP_NAME":"name"})
    issuers = issuers.merge(default_cmp, on='name',how='left')           
    issuers['date'] = issuers['date'].fillna(datetime(2016,12,31))    
    issuers['df'] = issuers['df'].fillna(0)               
     
    issuers_2016 = issuers[issuers['date']>'2016-01-01']
    issuers_2016_ndf = issuers[issuers['df']==0]
    
    
    all_ndf = get_total(start_date='2016-01-01',end_date='2016-12-31')
    issuers_2016_ndf_ = issuers_2016_ndf.merge(all_ndf, on='name', how='left')
    issuers_2016_ndf_ = issuers_2016_ndf_.fillna(0)
    insert_record = json.loads(issuers_2016_ndf_.to_json(orient='records'))
    collection = db.samples2016
    collection.insert_many(insert_record)    
    
    
    length = len(issuers_2016)
    for x in range(length):
        iss = issuers_2016.iloc[x]

        
        
        
        
        
    start = datetime(2015,1,1)
    start_date = (start.strftime('%Y-%m-%d'))
    end = datetime(2016,12,31)
    delta = end - start
    
    aDay = timedelta(days=+1)
    
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
#    db.pingan_total.delete_many({})
#    pingan_trust16()
#    export_pingan_trust16()
#    sued_between_holder()
#    time_searies_all()
    ret = zhixing_count('2015-02-20','2016-12-20',"银基烯碳新材料股份有限公司")

    
    
