# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 10:57:13 2017

@author: Administrator
"""

import pymysql
import pandas as pd

conn = pymysql.connect(host='192.168.10.43', port=3306, user='kejinuser', passwd='kejinuser',db='kejinTest')

cur = conn.cursor()

#cur.execute("SELECT * FROM d_a_mobile limit 100")
cur.execute("SELECT COUNT(*) AS CNT,CITY FROM kejinTest.d_a_mobile group by CITY order by CNT DESC;")
for r in cur.fetchall():
    if r[0] > 100:
        print(r)
cur.close()

conn.close()
