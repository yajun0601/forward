#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 22 16:26:29 2017

@author: yajun
"""

import matplotlib.pyplot as plt
import numpy as np

def sigmoid(h):
    return 1.0 / (1.0 + np.exp(-h))

h = np.arange(-20, 20, 0.1) # 定义x的范围，像素为0.1
s_h = sigmoid(h) # sigmoid为上面定义的函数
plt.plot(h, s_h)
plt.axvline(0.0, color='k') # 在坐标轴上加一条竖直的线，0.0为竖直线在坐标轴上的位置
plt.axhspan(0.0, 1.0, facecolor='1.0', alpha=1.0, ls='dotted') # 加水平间距通过坐标轴
plt.axhline(y=0.5, ls='dotted', color='k') # 加水线通过坐标轴
plt.axhline(y=0, ls='dotted', color='r') # 加水线通过坐标轴
plt.axhline(y=1, ls='dotted', color='r') # 加水线通过坐标轴
plt.yticks([0.0, 0.5, 1.0]) # 加y轴刻度
plt.ylim(-0.1, 1.1) # 加y轴范围
plt.xlabel('h')
plt.ylabel('$S(h)$')
plt.show()