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

client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.bonds
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

dest_db.bond_profit.insert_many(input_data.find())
dest_db.bond_balance.insert_many(db.bondBalance.find())
dest_db.issuers_info.insert_many(db.issuers_info.find())

dest_db.default_ratios.insert_many(db.default_ratios.find())
dest_db.default_2016.insert_many(db.default_2016.find())

