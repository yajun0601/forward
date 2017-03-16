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
AA_minus_2016 = df[(df['发布日期']>='2016-01-01')&(df['信用评级']=='AA-')]

record2016 = df[df['发布日期']>='2016-01-01']
record2015 = df[df['发布日期']<'2016-01-01']

AA15=record2015[record2015['信用评级']=='AA']
NAA15 = record2015[record2015['信用评级']!='AA']
AA15_merge=AA15.merge(NAA15,how='left',on='公司名称').fillna(0)
AA15_only=AA15_merge[AA15_merge['信用评级_y']==0]['公司名称'].drop_duplicates()
AA15_only.to_csv('AA15_only.csv')


AA1516 = df[df['信用评级']=='AA']
NAA1516 = df[df['信用评级']!='AA']
AA1516_merge=AA1516.merge(NAA1516,how='left',on='公司名称').fillna(0)
AA_only1516=AA1516_merge[AA1516_merge['信用评级_y']==0]['公司名称'].drop_duplicates()
AA_only1516.to_csv('AA_only1516.csv')

#16 only AA
AA16=record2016[record2016['信用评级']=='AA']
NAA16 = record2016[record2016['信用评级']!='AA']
AA16_merge=AA16.merge(NAA16,how='left',on='公司名称').fillna(0)
AA16_only=AA16_merge[AA16_merge['信用评级_y']==0]['公司名称'].drop_duplicates()
AA16_only.to_csv('AA16_only.csv')

# 16 only AA-
AA_16=record2016[record2016['信用评级']=='AA-']
NAA_16 = record2016[record2016['信用评级']!='AA-']
AA_16_merge=AA_16.merge(NAA_16,how='left',on='公司名称').fillna(0)
AA_16_only=AA_16_merge[AA_16_merge['信用评级_y']==0]['公司名称'].drop_duplicates()
AA_16_only.to_csv('AA_16_only.csv')


# "15 AA only" AND "16 AA- only"
AA_16o_list =set(AA_16_only)
AA15o_list = set(AA15_only)
AA15_AA_16_only = AA15o_list.intersection(AA_16o_list)
AA15_AA_16_only = pd.Series(list(AA15_AA_16_only))
AA15_AA_16_only.to_csv('AA15_AA_16_only.csv')


# 15 AA AND 16 AA
AA16o =set(AA16_only)
AA15o = set(AA15_only)
AA15o_AA16o=AA15o.intersection(AA16o)
AA15o_AA16o = pd.Series(list(AA15o_AA16o))
AA15o_AA16o.to_csv('AA15o_AA16o.csv')



# "15 AA only" AND "16 AA- only"
AA_16o_list =set(AA_16_only)
AA15o_list = set(AA15_only)
AA15_AA_16_only = AA15o_list.intersection(AA_16o_list)
AA15_AA_16_only = pd.Series(list(AA15_AA_16_only))
AA15_AA_16_only.to_csv('AA15_AA_16_only.csv')

last15 = pd.DataFrame()
grouped15 = record2015.groupby('公司名称')
for kk in grouped15.groups.keys():
    content = grouped15.get_group(kk)
    last = content[content['发布日期']==max(content['发布日期'])]
    print(last[last['信用评级']=='AA-'])
    last15 = last15.append(last[last['信用评级']=='AA'])
#last15.to_csv("last15.csv")    


first16 = pd.DataFrame()
grouped = record2016.groupby('公司名称')
for kk in grouped.groups.keys():
    content = grouped.get_group(kk)
    first = content[content['发布日期']==min(content['发布日期'])]
    print(first[first['信用评级']=='AA-'])
    first16 = first16.append(first[first['信用评级']=='AA-'])
#first16.to_csv('first16AA_.csv')    
last15AA = set(last15['公司名称'])
first16AA_=set(first16['公司名称'])
AA15L_AA_oFirst=last15AA.intersection(first16AA_)    
pd.Series(list(AA15L_AA_oFirst)).to_csv('AA15L_AA_oFirst.csv')