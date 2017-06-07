#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  2 20:09:10 2017

@author: zhengyajun
"""

from sklearn.datasets import load_iris
 
#导入IRIS数据集
iris = load_iris()

#特征矩阵
iris.data

#目标向量
iris.target

from sklearn.preprocessing import StandardScaler
#标准化，返回值为标准化后的数据
data_ = StandardScaler().fit_transform(iris.data)

from sklearn.preprocessing import MinMaxScaler

#区间缩放，返回值为缩放到[0, 1]区间的数据
data_sclr_ = MinMaxScaler().fit_transform(iris.data)

from sklearn.preprocessing import OneHotEncoder

#哑编码，对IRIS数据集的目标值，返回值为哑编码后的数据
OneHotEncoder().fit_transform(iris.target.reshape((-1,1)))

