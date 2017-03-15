#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 17:04:56 2017

@author: yajun
"""

import pandas as pd
import numpy as np
import math

DATA_FILE='data/发债公司信用评级（2015年1月1日-2016月12月31日）.xlsx'
df = pd.read_excel(DATA_FILE,sheetname=[0], parse_cols="A:B,D,E",header = 0,skip_footer=2)
df = df[0]
AA_201516 = df[df['信用评级']=='AA']['公司名称']
AA_minus_201516 = df[df['信用评级']=='AA-']['公司名称']
AA_plus_201516 = df[df['信用评级']=='AA+']['公司名称']
AA_201516 = AA_201516.drop_duplicates()
AA__2016 = df[(df['发布日期']>='2016-01-01')&(df['信用评级']=='AA-')]

record2016 = df[df['发布日期']>='2016-01-01']
record2015 = df[df['发布日期']<'2016-01-01']
AA15=record2015[record2015['信用评级']=='AA']
NAA15 = record2015[record2015['信用评级']!='AA']

 AA15.merge(NAA15,on='公司名称')
 
AA15.join(NAA15,lsuffix='_caller', rsuffix='_other',how='inner',on='公司名称')
AA15.merge(NAA15)
AA15
NAA15
AA15.merge(NAA15,on='公司名称')