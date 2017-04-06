# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 10:57:13 2017

@author: Administrator
"""

import pymysql

conn = pymysql.connect(host='192.168.10.60', port=3306, user='yajun', passwd='0601yajun',db='creditDB')

cur = conn.cursor()

cur.execute("SELECT * FROM cashflows")

for r in cur.fetchall():
           print(r)

cur.close()

conn.close()
