#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 08:23:34 2017

@author: zhengyajun
"""

import pandas as pd
from numpy  import *
from pymongo import *
import json

#client = MongoClient("mongodb://192.168.10.60:27017/")
client = MongoClient("mongodb://1t611714m7.iask.in:12471/")
db = client.stocks
#input_data = db.bond_profit
#data = pd.DataFrame(list(input_data.find()))
#

dest_client = MongoClient("mongodb://127.0.0.1:27017/")
dest_db = dest_client.bonds

dest_data = dest_db.issuers
#out = pd.DataFrame(list(dest_data.find()))

rptDate=['20131231','20141231','20151231']

input_data = db.bond_profit
#data = pd.DataFrame(list(input_data.find()))
dest_db.bond_cashflow.insert_many(db.bond_cashflow.find())
dest_db.bond_profit.insert_many(db.bond_profit.find())
dest_db.bond_balance.insert_many(db.bondBalance.find())
dest_db.issuers_info.insert_many(db.issuers_info.find())
dest_db.default_ratios.insert_many(db.default_ratios.find())
dest_db.default_2016.insert_many(db.default_2016.find())



# insert dest db as condition limits
dest_db.bond_cashflow.insert_many(db.bond_cashflow.find({"rptDate" : "20161231"}))
dest_db.bond_profit.insert_many(db.bond_profit.find({"rptDate" : "20161231"}))
dest_db.bond_balance.insert_many(db.bondBalance.find({"rptDate" : "20161231"}))

df = pd.DataFrame(list(db.bond_cashflow.find({"rptDate" : "20161231"})))
query = db.bond_balance.find({"$or":[{"rptDate" : "20161231"},{"rptDate" : "20151231"}]},{'_id':0,'COMP_NAME':0,'CITY':0,'LISTINGORNOT':0,'PROVINCE':0})
balance=pd.DataFrame(list(query))
shape(balance)