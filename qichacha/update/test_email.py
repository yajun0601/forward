#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 10:32:57 2017

@author: yajun
"""

import my_email as my
import os
import pandas as pd
my.my_print('a test')

mailto_list=["zhengyajun@88mmmoney.com"]#,"zqf_btc@126.com"
output_dir = os.getcwd()
attach = 'finance2017.xlsx'
df = pd.read_excel("%s/%s"%(output_dir,attach))
namelist = list(set(df['COMP_NAME']))
content = '\n'.join(namelist)
subject = "新增报告"
if my.send_mail(mailto_list,subject, output_dir, attach, content):
    print('Succeed send email:'+content)
