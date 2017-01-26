#-*- coding: UTF-8 -*- 

'''
1、读取指定目录下的所有文件
2、读取指定文件，输出文件内容
3、创建一个文件并保存到指定目录
'''
import pandas as pd
import numpy as np
import math


DATA_FILE='数据：制造业-信用债.xlsx'
#DATA_FILE='/Users/zhengyajun/Documents/forward/modle/数据：制造业-信用债.xlsx'
MODLE_FILE_NAME='机构主体信用风险评级模型-制造业.xlsx'
#MODLE_FILE_NAME='/Users/zhengyajun/Documents/forward/modle/机构主体信用风险评级模型-制造业.xlsx'
def getModle():
    fixedEvaluation = pd.read_excel(MODLE_FILE_NAME,sheetname=[0], header = 0, skiprows = [0])
    industryTbl = pd.read_excel(MODLE_FILE_NAME,sheetname=[1], index_col = 2,parse_cols="B:L",header = 3)
    trendTbl = pd.read_excel(MODLE_FILE_NAME,sheetname=[2], header = 2,skiprows=[0])
    fluctuationTbl = pd.read_excel(MODLE_FILE_NAME,sheetname=[3], header = 2,skiprows=[0])
    fixedScoreTble = pd.read_excel(MODLE_FILE_NAME,sheetname=[4], header = 0,skiprows=[0])
    
    df=pd.read_excel(DATA_FILE,sheetname=[1], header = 0,index_col=0,verbose=True)
    df[1].head().index
    df[1].head().columns
    
    df[1].head().describe()
    df[1].head().loc[:,['证券代码','定量总得分']]
    
    for i in range(df[1].head().iloc[1].count()):
        print(df[1].head().iloc[1][i])
    
    head = df[1].head()    
    head.values[0][1:40].reshape(13,3)

industryTbl = pd.read_excel(MODLE_FILE_NAME,sheetname=[1], parse_cols="C:L",header = 2,convert_float=False)
industryTbl = industryTbl[1]
def calcIndustry(id,data):
#    print(id)
#    print(data)
    industryScore=np.zeros(13)
    for i in range(13):
        if i in (3,5,6):
            if data[i][2] > industryTbl.values[i][0]:
                industryScore[i] = 0
            elif data[i][2] <= industryTbl.values[i][9]:
                industryScore[i] = 100
        else:
            if data[i][2] < industryTbl.values[i][0]:
                industryScore[i] = 0
            elif data[i][2] >= industryTbl.values[i][9]:
                industryScore[i] = 100

    for i in range(13):
        for j in range(9):
            if i in (3,5,6):
                if data[i][2] <= industryTbl.values[i][j] and data[i][2] > industryTbl.values[i][j + 1]:
                    industryScore[i] = industryTbl.columns[j]
                    break
            else:
                if data[i][2] >= industryTbl.values[i][j] and data[i][2] < industryTbl.values[i][j + 1]:
                    industryScore[i] = industryTbl.columns[j]
                    break;
                
#        print("%d:%d:%f"%(i,j,industryScore[i]))
            
    return (industryScore)


trendTbl = pd.read_excel(MODLE_FILE_NAME,sheetname=[2], parse_cols="C:L",header = 2,convert_float=False)[2]
def calcTrend(id,data):
    growthScore=np.ones(13)
    growth=np.ones(13)
    for i in range(13):
        growth[i] = 100*(data[i][2] - data[i][0])/abs(data[i][0])
    
    for i in range(13):
        if i in (3,5,6):
            if growth[i] > trendTbl.values[i][0]:
                growthScore[i] = 0
            elif growth[i] <= trendTbl.values[i][9]:
                growthScore[i] = 100
        else:
            if growth[i] < trendTbl.values[i][0]:
                growthScore[i] = 0
            elif growth[i] >= trendTbl.values[i][9]:
                growthScore[i] = 100

    for i in range(13):
        for j in range(9):
            if i in (3,5,6):
                if growth[i] <= trendTbl.values[i][j] and growth[i] > trendTbl.values[i][j + 1]:
                    growthScore[i] = trendTbl.columns[j]
                    break
            else:
                if growth[i] >= trendTbl.values[i][j] and growth[i] < trendTbl.values[i][j + 1]:
                    growthScore[i] = trendTbl.columns[j]
                    break;
                
#        print("%d:%d:%f"%(i,j,growthScore[i]))
            
    return (growthScore)
fluctuationTbl = pd.read_excel(MODLE_FILE_NAME,sheetname=[3], parse_cols="C:L",header = 2,convert_float=False)[3]
def STDEV_P(a):
        l=len(a)
        m=sum(a)/l
        d=0
        for i in a: d+=(i-m)**2
        return (d*(1/l))**0.5
def STDEV_S(a):
        l=len(a)
        m=sum(a)/l
        d=0
        for i in a: d+=(i-m)**2
        return (d/(l-1))**0.5

def calcFluctuation(id,data):
    fluctuation=np.ones(13)
    abs_avg=np.ones(13)
    std = np.ones(13)
    growth = np.ones(13)
    growthScore=np.ones(13)
    for i in range(13):
        abs_avg[i] = abs(np.mean(data[i]))
        if abs_avg[i] == 0:
            abs_avg[i] = 0.0001 
        std[i] = STDEV_S(data[i])
        growth[i] = std[i]/abs_avg[i]
    
    for i in range(13):
        if growth[i] < fluctuationTbl.values[i][9]:
            growthScore[i] = 100
        elif growth[i] >= fluctuationTbl.values[i][0]:
            growthScore[i] = 0
    for i in range(13):
        for j in range(9):
            if growth[i] <= fluctuationTbl.values[i][j] and growth[i] > fluctuationTbl.values[i][j + 1]:
                growthScore[i] = fluctuationTbl.columns[j]
                break;
                
#        print("%d:%d:%f"%(i,j,growthScore[i]))
        
    return growthScore
    

    
fixedScoreTble = pd.read_excel(MODLE_FILE_NAME,sheetname=[4], parse_cols="D:L",header = 0,convert_float=False)[4]
def calcTotal(data):
    weight_sum = np.ones(len(data))
    for i in range(len(data)):
        weight_sum[i] = data[i]*fixedScoreTble['权重'].values[i]
        
    return np.sum(weight_sum)
    
def process():
    df=pd.read_excel(DATA_FILE,sheetname=[1], header = 0,index_col=0,convert_float=False)[1]
#    df0 = df[1].fillna(0)
#    df = df.head(100)
#    resultList=np.zeros(df.index.size)
    resultList=[]
    idList=[]
    for i in range(df.index.size):
        for nan in range(1,40):
            if math.isnan(df.values[i][nan]):
#                print(df[1].values[i])
                df.values[i][40]=np.nan
                break
        if nan < 39: # for nan results
            idList.append(df.index[i])
            resultList.append([df.values[i][0],0])
            print("%s:%s"%(df.index[i],df.values[i][0]))
            continue
        ID=df.index[i]
        df.values[i][0]
        data=df.values[i][1:40].reshape(13,3)
# handle nulls        
#        print(data)
        industryScore = calcIndustry(ID,data)
        trendScore = calcTrend(ID,data)
        fluncScore = calcFluctuation(ID,data)
        a = np.append(industryScore,trendScore)
        b= np.append(a,fluncScore)
        idList.append(ID)
        resultList.append([df.values[i][0],calcTotal(b)])
        
#        print("%s:%f"%(ID,resultList[i]))
    resultdf=pd.DataFrame(resultList,idList,columns=['NAME','Score'])
    with pd.ExcelWriter('result.xls') as writer:
        resultdf.to_excel(writer,sheet_name=str(0))

if __name__ == "__main__":
    process()
