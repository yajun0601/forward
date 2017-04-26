# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 22:35:18 2017

@author: Administrator
"""

from WindPy import *
import pandas as pd
from numpy  import *
from pymongo import *
import json

w.start()
client = MongoClient()
client = MongoClient("mongodb://192.168.10.70:27017/")
db = client.issures



w.start()

data = w.wss("000005.SZ", "comp_name,province,city,listingornot,nature,industry_CSRC12,industry_gics","industryType=2")
