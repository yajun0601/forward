#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 10:13:19 2017

@author: yajun
"""


def roc_plot(n,net, test):
    from sklearn.metrics import roc_curve, roc_auc_score #导入ROC曲线函数
    import matplotlib.pyplot as plt
    predict_result = net.predict(test[:,:-1]).reshape(len(test))
    fpr, tpr, thresholds = roc_curve(test[:,-1], predict_result, pos_label=1)
    auc = roc_auc_score(test[:,-1], predict_result, average="macro", sample_weight=None)
    plt.plot(fpr, tpr, linewidth=2, label = "AUC=%f"%(auc)) #作出ROC曲线
    plt.xlabel('False Positive Rate') #坐标轴标签
    plt.ylabel('True Positive Rate') #坐标轴标签
    plt.ylim(0,1.05) #边界范围
    plt.xlim(0,1.05) #边界范围
    plt.legend(loc=4) #图例
    return plt #显示作图结果