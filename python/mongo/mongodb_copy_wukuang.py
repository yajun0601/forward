# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 15:09:19 2017

@author: yajun
"""

"""
Created on Thu Apr 27 08:23:34 2017

@author: zhengyajun
"""

import pandas as pd
from numpy  import *
from pymongo import *
import json
client = MongoClient("mongodb://1t611714m7.iask.in:22839/")
db = client.bonds
query= db.yeild_csi_std_samples.find().limit(5)
df = pd.DataFrame(list(query))
print(df)

try:# Python 3.x
    from urllib.parse import quote_plus
except ImportError:# Python 2.x
    from urllib import quote_plus
from pymongo import MongoClient

#Example
user = 'worker'
password = '123'
host = '127.0.0.1:27017'
#Code
uri = "mongodb://%s:%s@%s" % (
    quote_plus(user), quote_plus(password), host)
dest_client = MongoClient(uri)
dest_db = dest_client.bonds

dest_db.yeild_csi_std_samples.insert_many(db.yeild_csi_std_samples.find())
