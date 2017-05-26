#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 25 00:21:30 2017

@author: zhengyajun
"""

import pandas as pd

DATA_FILE='../data/发债公司信用评级（2015年1月1日-2016月12月31日）.xlsx'
df = pd.read_excel(DATA_FILE,sheetname=[0], header = 0)[0]

print(df)