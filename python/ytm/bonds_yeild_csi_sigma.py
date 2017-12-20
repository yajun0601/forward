# -*- coding: utf-8 -*-
"""
Created on Sat Nov 25 22:04:51 2017

@author: Administrator
"""

import pymongo
import datetime
import pandas as pd
import matplotlib as mpl
import numpy as np
import matplotlib.pyplot as plt
import json
#%matplotlib inline
#plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
#mpl.rcParams['axes.unicode_minus']=False #用来正常显示负号
mpl.rc('xtick', labelsize=10) #设置坐标轴刻度显示大小
mpl.rc('ytick', labelsize=10) 
client         = pymongo.MongoClient("mongodb://192.168.10.60:27017/")
db              = client.bonds
def get_bond_clean_df(bondType='default'):
    
    if bondType == 'default':
        collection = db.default_ytm
    elif bondType == 'matured':
        collection = db.matured_bond_ytm
    elif bondType == '2018private':
        collection = db.matured_private_bond_ytmb_2018
    elif bondType == '2018_all':
        collection = db.maturity_yield_csi_2018
    
    
    query           =  collection.find({},{"_id":0})
    bonds_ytm = pd.DataFrame(list(query))
#    bonds_ytm = bonds_ytm.fillna(axis='columns',method='backfill')
    #查看公司重复项的清单。
    #def check_duplicate_company():
    #duplicate_company = bonds_ytm[bonds_ytm.duplicated('issuer')].issuer
    #    return duplicate_company
    
    #check_duplicate_company()
    
    # 删除重复项
    #def del_duplicates():
    bonds_ytm.sort_values(by='end_date', inplace =True) #列表按照时间排序
    bonds_ytm.drop_duplicates(['issuer'], inplace=True)
    bonds_ytm[bonds_ytm.duplicated('issuer')]
    #    len(bonds_ytm[bonds_ytm.duplicated('issuer')])
    #    if len(res)==0 :
    #        print('Duplicate items have been deleted')
    #    else:
    #        print('There are duplicates')
    #    return bonds_ytm
    
    #del_duplicates()
    
    # 单独保存公司基本信息和债券价格信息
    data = bonds_ytm.T
    company_info = data.iloc[-3:]
    bonds_df = data.drop(['code','end_date','issuer'])
    company_names = company_info.iloc[-1:].T # 获取公司名字清单
    company_names_dic = company_names.to_dict(orient='dict')['issuer']
    company_info.rename(columns=company_names_dic, inplace=True)
    bonds_df.rename(columns=company_names_dic, inplace=True)
    
    # 缺失值处理
    
    #def del_missing_value():
    null_dict = dict(bonds_df.isnull().sum())
    demo = pd.DataFrame(null_dict,index=['missing_value']) # 对缺失值构建新dataframe
    bonds_new = pd.concat([bonds_df,demo], axis=0).T # 纵向合并
    bonds_new.sort_values(by='missing_value', inplace =True,ascending=False) # 列表按照缺失值倒序排序
    thresh = len(bonds_new.columns)*0.98 # 设定阈值
    drop_index = bonds_new.loc[bonds_new['missing_value']>thresh].index
    bonds_clean = bonds_new.drop(drop_index).copy()# 删除缺失值较多的列
    bonds_clean_df = bonds_clean.drop(labels=['missing_value'],axis=1).T # 删除missing_value并转置
    #    return bonds_clean_df
    
    #del_missing_value()
        
    # 列表按照时间排序
    #def sort_date():
    bonds_clean_df.index = bonds_clean_df.index.astype('int')
    bonds_clean_df = bonds_clean_df.sort_index()
    #    return
    
    #sort_date()
        
    # 以均值填充缺失值
    #def mean_imputer():
    bonds_col = list(bonds_clean_df.columns)
    bonds_clean_df = bonds_clean_df.sort_index()
    from sklearn.preprocessing import Imputer
    imr = Imputer(missing_values='NaN', strategy='mean', axis=0)  # 针对axis=0 列来处理
    imr = imr.fit(bonds_clean_df[bonds_col])
    bonds_clean_df[bonds_col] = imr.transform(bonds_clean_df[bonds_col])
    
    return bonds_clean_df
#    return




def Price_and_Volatility_visualization(name, window=10):
    import matplotlib as mpl
    bonds_clean_df = get_bond_clean_df(bondType='matured')
    mpl.rcParams['font.sans-serif'] = ['Droid Sans Fallback']  # chinese charactor
    s = 0
    e = 247
    fig = plt.figure(figsize=(15,5),dpi=100)

    # 设置左右标签
    yLeftLabel = 'YTM'
    yRightLable = '10 Days Sigma'

    ax1 = fig.add_subplot(111)

    # 绘制5日标准线走势
    ax1.plot(bonds_clean_df[s:e].index,bonds_clean_df[s:e][name],color ="blue", label=yLeftLabel)
    legend1=ax1.legend(loc=(.88,1.1), fontsize=10,shadow=True)

    ax1.grid(True)
    ax1.set_title( '{code}-{company_name}-{end_date}'.format(code=company_info[name]['code'] ,company_name=name  ,end_date=company_info[name]['end_date'] ), fontsize = 15)
    plt.xlabel('Time',fontsize=15)
    # 设置双坐标轴，右侧Y轴
    ax2=ax1.twinx()
    ax1.set_ylabel('YTM',fontsize=15)
    ax2.set_ylabel('10 Days Sigma',fontsize=15)

    # 绘制5日标准线走势
    ax2.plot(bonds_clean_df[s:e].index, bonds_clean_df[s:e][name].rolling(window=window, center=False).std(),color='green', label= yRightLable)

    legend2=ax2.legend(loc=(.88,1.03),fontsize=10 ,shadow=True)
    return

def max_std(name):
    s = 0
    e = 247
    max_std = bonds_clean_df[s:e][name].rolling(window=10, center=False).std().max(axis=0)
    return max_std
    
def insert_database(dataframe, bondType = 'default'):
    pd.set_option('precision', 4)
    ytm_max_std = dataframe.rolling(window=5, center=False).std().max(axis=0)
    ytm_max_std_top = list()
    for x in ytm_max_std.values:
        if x > 10:
            ytm_max_std_top.append(10)
        else:
            ytm_max_std_top.append(x)
            
    ytm_max_std_df = pd.DataFrame(data=[ytm_max_std.index,ytm_max_std_top]).T
    ytm_max_std_df.rename(columns={0:'name',1:'std'}, inplace=True)
    
    db_ytm              = client.qichacha_new
    collection = db_ytm.shixin_zhixing_defend
    query           =  collection.find({},{'_id':0,'name':1,'defendant':1,'shixin':1,'zhixing':1})
    shixin_zhixing = pd.DataFrame(list(query))
    
    df_samples = ytm_max_std_df.merge(shixin_zhixing,on='name',how='left')

#   state-owned company
    db_comp = client.company_data
    query = db_comp.all_ratio_data.find({"NATURE":{"$regex":"国有企业"}},{"_id":0,"COMP_NAME":1})
    national_df = pd.DataFrame(list(query))
    national_df.drop_duplicates(inplace=True)
    national_df.columns=['name']
    national_df['national'] = 1
               
    df_samples = df_samples.merge(national_df,on='name',how='left')   
    df_samples['national'] = df_samples['national'].fillna(0)

    if bondType == 'default':
        df_samples['df'] = 1
#        df_samples = df_samples.fillna(df_samples.mean()) # do not fill anything
    elif bondType == 'matured':
        df_samples['df'] = 0
#        df_samples = df_samples.fillna(df_samples.mean()) # do not fill anything
#    elif  bondType == '2018private':
#        df_samples = df_samples.fillna(0)
    elif  bondType == '2018_all':
        df_samples = df_samples.fillna(0)
    
    insert_record = json.loads(df_samples.to_json(orient='records'))
    db.yeild_csi_std_samples.insert_many(insert_record)

def insert_default_and_matured():    
    bondType = 'default'
    df = get_bond_clean_df(bondType)
    insert_database(df, bondType)   
    bondType = 'matured'
    df = get_bond_clean_df(bondType)
    insert_database(df,bondType)      
    
    bondType = '2018_all'
    df = get_bond_clean_df(bondType)
    insert_database(df,bondType)     
#    bondType = '2018private'
#    df = get_bond_clean_df(bondType)
#    insert_database(df,bondType)      
 
def insert_2018_private_bond():
    pd.set_option('precision', 2)
    ytm_max_std = bonds_clean_df.rolling(window=10, center=False).std().max(axis=0)
    ytm_max_std_df = pd.DataFrame(data=[ytm_max_std.index,ytm_max_std.values]).T
    ytm_max_std_df.rename(columns={0:'name',1:'std'}, inplace=True)
    
    db_ytm              = client.qichacha_new
    collection = db_ytm.shixin_zhixing_defend
    query           =  collection.find({},{'_id':0,'name':1,'defendant':1,'shixin':1,'zhixing':1})
    shixin_zhixing = pd.DataFrame(list(query))
    
    df_samples = ytm_max_std_df.merge(shixin_zhixing,on='name')
#    df_samples['df'] = 0
    df_samples = df_samples.fillna(0)
    
    insert_record = json.loads(df_samples.to_json(orient='records'))
    db.ytm_std_samples_2018.insert_many(insert_record)       

def draw_default_bond():
    for comp in (bonds_clean_df.columns):
        Price_and_Volatility_visualization(comp,5)
    return
def available_font():
    from matplotlib.font_manager import FontManager
    import subprocess
    fm = FontManager()
    mat_fonts = set(f.name for f in fm.ttflist)
    #print(mat_fonts)
    output = subprocess.check_output('fc-list :lang=zh -f "%{family}\n"', shell=True)
    #print( '*' * 10, '系统可用的中文字体', '*' * 10)
    #print (output)
    zh_fonts = set(f.split(',', 1)[0] for f in output.decode('utf-8').split('\n'))
    available = mat_fonts & zh_fonts
    print ('*' * 10, '可用的字体', '*' * 10)
    for f in available:
         print (f)
     
if __name__ == "__main__":
    Price_and_Volatility_visualization('中科云网科技集团股份有限公司',5)
#    insert_database()
#    check_duplicate_company()
#    del_duplicates()
#    del_missing_value()
#    sort_date()
#    mean_imputer()

#    name = input('输入需要查询公司的名字')
#    name = '天津市天联滨海复合材料有限公司'
#    Price_and_Volatility_visualization(name)
#    max_std('中科云网科技集团股份有限公司')
#    name = '华盛江泉集团有限公司'
#    Price_and_Volatility_visualization(name)
    
    Price_and_Volatility_visualization('浙江中欣氟材股份有限公司')
        
#        print(comp[0])
#    
#    for name in bonds_clean_df.columns:
#        print("%s: \t%f"%(name,max_std(name)))
#        Price_and_Volatility_visualization(name)

    

