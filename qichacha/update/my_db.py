#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 10:17:25 2017

@author: yajun
"""

from pymongo import MongoClient
import pandas as pd

def getCompanyList():
    client = MongoClient("mongodb://localhost:27017/")
#    client = MongoClient("mongodb://1t611714m7.iask.in:22839")
    db_comp = client.company_data
    collection = db_comp.distinct_company_list
    query = collection.find({},{"_id":0,"CODE":0})
    company_df = pd.DataFrame(list(query))
    MongoClient.close(client)
    return company_df