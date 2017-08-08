#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 15:48:11 2017

@author: yajun
"""
import numpy
import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.bonds

def get_rating():
    query = db.ratings.find({},{'_id':0})
    all_rating = pd.DataFrame(list(query))
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('AAA+','AA')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('AAA-','AA')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('AA+','AA')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('AA-','AA')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('A+','A')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('A-','A')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('A-1','A')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('BBB+','BBB')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('BBB-','BBB')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('BB+','BB')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('BB-','BB')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('B+','B')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('B-','B')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('AA+','AA')
    all_rating['LATESTISSURERCREDITRATING2'] = all_rating['LATESTISSURERCREDITRATING2'].replace('AA+','AA')
    
    print(all_rating.groupby('LATESTISSURERCREDITRATING2').size().sort_values())
    sorted_rating = all_rating.sort_values(['code','rptDate'], ascending=False)
    sorted_rating = sorted_rating.reset_index(drop=True)
    
    # fill and drop 'None' value
    for j in range(int(len(sorted_rating)/14)):
        for i in range(14):
            index = 14*j + i
            if sorted_rating.iloc[index]['LATESTISSURERCREDITRATING2'] is None:
                if i != 0:
                    sorted_rating.iloc[index]['LATESTISSURERCREDITRATING2'] = sorted_rating.iloc[index-1]['LATESTISSURERCREDITRATING2']
    rating = sorted_rating.dropna()    
    # print the count after drop null
    print(rating.groupby('rptDate').size().sort_values())
    
    ret_rating = rating[rating['rptDate'] == '20160630']
    ret_rating['rptDate'] = ret_rating['rptDate'].replace('20160630','20151231')
    
    rating_map = pd.DataFrame(list(range(9)), index=['AAA','AA','A','BBB','BB','B','CCC','CC','C'])
    
    for r in rating_map.index:
        print(r)
        ret_rating['LATESTISSURERCREDITRATING2'] = ret_rating['LATESTISSURERCREDITRATING2'].replace(r,rating_map.T[r])

'''
AAA     0
AA      1
A       2
BBB     3
BB      4
B       5
CCC     6
CC      7
C       8
'''
def dense_to_one_hot(labels_dense, num_classes):
  """Convert class labels from scalars to one-hot vectors."""
  print(labels_dense)
  num_labels = labels_dense.shape[0]
  index_offset = numpy.arange(num_labels) * num_classes
  labels_one_hot = numpy.zeros((num_labels, num_classes))
  labels_one_hot.flat[index_offset + labels_dense.ravel()] = 1
  return labels_one_hot

def get_issuer(kind = 'all'):
    query = db.bond_balance.find({'rptDate':'20151231'},{'_id':0,'COMP_NAME':1, 'code':1})
    issuer = pd.DataFrame(list(query)) #Convert the input to an array.
    
    if kind == 'private':
        query = db.private_enterprise.find({},{'_id':0, 'code':0, '证券简称':0})
        private = pd.DataFrame(list(query)).drop_duplicates()
        issuer = private.merge(issuer, how='inner', on='COMP_NAME')
        
    return issuer

def get_report():
    client = MongoClient("mongodb://192.168.10.60:27017/")
    db = client.bonds
    rptDates={"$or":[{"rptDate" : "20161231"},{"rptDate" : "20151231"}]}
    
    query = db.bond_balance.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
    balance = pd.DataFrame(list(query)) #Convert the input to an array.
    
    query = db.bond_cashflow.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
    cashflow = pd.DataFrame(list(query)) #Convert the input to an array.
    
    query = db.bond_profit.find(rptDates,{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
    profit = pd.DataFrame(list(query)) #Convert the input to an array.
    
    tmp = balance.merge(cashflow, on=['code','rptDate'])
    financial_report = tmp.merge(profit, on=['code','rptDate'])  # finacial report of 2015
                            
    private_report = financial_report.merge(get_issuer(), on='code')
    
    report = private_report.dropna(axis = 1, how='any', thresh=int(len(private_report)*0.25))                   
    report = report.fillna(report.mean())
    
    report2015 = report[report['rptDate'] == '20151231']
    report2015 = report2015.drop(['rptDate','COMP_NAME'], axis=1)
    report2016 = report[report['rptDate'] == '20161231']
    report2016 = report2016.drop(['rptDate','COMP_NAME'], axis=1)
    
    client.close()
    return report2015,report2016

report2015,report2016 = get_report()
labels_array = numpy.array([0,1,2,3,4,5,6,7,8])
labels = dense_to_one_hot(labels_array, 9)



