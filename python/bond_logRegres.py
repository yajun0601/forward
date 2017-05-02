'''
Created on Oct 27, 2010
Logistic Regression Working Module
@author: Peter
'''
from numpy import *
import pandas as pd
from pymongo import *

def loadDataSet():
    dataMat = []; labelMat = []
    fr = open('testSet.txt')
    for line in fr.readlines():
        lineArr = line.strip().split()
        dataMat.append([1.0, float(lineArr[0]), float(lineArr[1])])
        labelMat.append(int(lineArr[2]))
    return dataMat,labelMat

def sigmoid(inX):
    return longfloat(1.0/(1+exp(-inX)))

def gradAscent(dataMatIn, classLabels):
    dataMatrix = mat(dataMatIn)             #convert to NumPy matrix
    labelMat = mat(classLabels).transpose() #convert to NumPy matrix
    m,n = shape(dataMatrix)
    alpha = 0.001
    maxCycles = 500
    weights = ones((n,1))
    for k in range(maxCycles):              #heavy on matrix operations
        h = sigmoid(dataMatrix*weights)     #matrix mult
        error = (labelMat - h)              #vector subtraction
        weights = weights + alpha * dataMatrix.transpose()* error #matrix mult
    return weights

def plotBestFit(weights):
    import matplotlib.pyplot as plt
    dataMat,labelMat=loadDataSet()
    dataArr = array(dataMat)
    n = shape(dataArr)[0] 
    xcord1 = []; ycord1 = []
    xcord2 = []; ycord2 = []
    for i in range(n):
        if int(labelMat[i])== 1:
            xcord1.append(dataArr[i,1]); ycord1.append(dataArr[i,2])
        else:
            xcord2.append(dataArr[i,1]); ycord2.append(dataArr[i,2])
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.scatter(xcord1, ycord1, s=30, c='red', marker='s')
    ax.scatter(xcord2, ycord2, s=30, c='green')
    x = arange(-3.0, 3.0, 0.1)
    y = (-weights[0]-weights[1]*x)/weights[2]
    ax.plot(x, y)
    plt.xlabel('X1'); plt.ylabel('X2');
    plt.show()

def stocGradAscent0(dataMatrix, classLabels):
    m,n = shape(dataMatrix)
    alpha = 0.01
    weights = ones(n)   #initialize to all ones
    for i in range(m):
        h = sigmoid(sum(dataMatrix[i]*weights))
        error = classLabels[i] - h
        weights = weights + alpha * error * dataMatrix[i]
    return weights

#ww=array
#t_data=array
#d_index = 0
def stocGradAscent1(dataMatrix, classLabels, numIter=150):
    m,n = shape(dataMatrix)
    weights = ones(n)   #initialize to all ones
    for j in range(numIter):
        dataIndex = list(range(m))
        for i in range(m):
            alpha = 4/(1.0+j+i)+0.0001    #apha decreases with iteration, does not 
            randIndex = int(random.uniform(0,len(dataIndex)))#go to 0 because of the constant
#            print(randIndex, sum(dataMatrix[randIndex]*weights))

            h = sigmoid(sum(dataMatrix[randIndex]*weights))
            error = classLabels[randIndex] - h
            weights = weights + alpha * error * dataMatrix[randIndex]
            del(dataIndex[randIndex])
    return weights

def classifyVector(inX, weights):
    prob = sigmoid(sum(inX*weights))
    if prob > 0.5: return 1.0
    else: return 0.0

def colicTest():
    frTrain = open('horseColicTraining.txt'); frTest = open('horseColicTest.txt')
    trainingSet = []; trainingLabels = []
    for line in frTrain.readlines():
        currLine = line.strip().split('\t')
        lineArr =[]
        for i in range(21):
            lineArr.append(float(currLine[i]))
        trainingSet.append(lineArr)
        trainingLabels.append(float(currLine[21]))
    trainWeights = stocGradAscent1(array(trainingSet), trainingLabels, 1000)
    errorCount = 0; numTestVec = 0.0
    
    for line in frTest.readlines():
        numTestVec += 1.0
        currLine = line.strip().split('\t')
        lineArr =[]
        for i in range(21):
            lineArr.append(float(currLine[i]))
        if int(classifyVector(array(lineArr), trainWeights))!= int(currLine[21]):
            errorCount += 1
    errorRate = (float(errorCount)/numTestVec)
    print("the error rate of this test is: %f" % errorRate)
    print(trainWeights)
    return errorRate

def multiTest():
    numTests = 10; errorSum=0.0
    for k in range(numTests):
        errorSum += colicTest()
    print("after %d iterations the average error rate is: %f" % (numTests, errorSum/float(numTests)))

def bondTest(data):
#    keep 4 dot points
    default = round(tSet[tSet['df'] == 1],4) 
    normal = round(tSet[tSet['df'] == 0],4)
    
    normal_len = int(len(normal)*0.7)
    default_len = int(len(default) * 0.7)
    training = normal[:normal_len].append(default[:default_len])
    
    trainingLabels = list(training['df'])
    trainingSet = training.drop(labels=['df'], axis=1)
    
    testSet = normal[normal_len:].append(default[default_len:])
    
#    trainingSet = trainingSet*100
    dataMat = trainingSet.values.astype('float64')
    trainWeights = stocGradAscent1(dataMat, trainingLabels, 10)
    
#    errorCount = 0; numTestVec = 0.0
    Normal_errorCount = 0;Default_errorCount = 0
    
    default_num = len(testSet[testSet['df'] == 1.0])
    normal_num = len(testSet[testSet['df'] == 0.0])
    testLabels = testSet['df']
    testSet = testSet.drop(labels=['df'], axis=1)
#    testSet = testSet * 100
    index = 0
    for line in testSet.values:
        index += 1
        df = trainingLabels[index]
        if int(classifyVector(line.astype('float64'), trainWeights))!= int(df):
            if int(df) == 0:
                Normal_errorCount += 1
            else:
                Default_errorCount += 1

    default_errorRate = (float(Default_errorCount)/default_num)
    normal_errorRate = (float(Normal_errorCount)/normal_num)
    print("default_num: %d,normal_num: %d"%(default_num, normal_num))
    print("default_errorRate: %d : %f, normal_errorRate:%d : %f" %(Default_errorCount,default_errorRate,Normal_errorCount,normal_errorRate))
    return Default_errorCount,Normal_errorCount
    
if __name__ == "__main__":
    from pymongo import *
    
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client.bonds
    input_data = db.model
    
    data = pd.DataFrame(list(db.model.find({},{'code':0,'_id':0}))) # 'INDUSTRY_GICSCODE':0
    INDUSTRY_GICSCODE = data['INDUSTRY_GICSCODE'].astype('float64')/1000
    data.drop('INDUSTRY_GICSCODE',axis = 1)
#    data['INDUSTRY_GICSCODE'] = INDUSTRY_GICSCODE
#    bondTest(data)
    tSet=data.dropna(axis=0,how='any',thresh=200) # drop if na is above 50
    tSet=tSet.dropna(axis=1,how='any', thresh=1000) # drop column if na is above 10000
    tSet.fillna(0.0,inplace=True)
    numTests = 20; 
    default_erSum=0.0;
    normal_erSum=0.0;
    for k in range(numTests):
        default_er,normal_er = bondTest(tSet)
        default_erSum += default_er
        normal_erSum += normal_er
        
    print("after %d iterations default_erSum is: %f, normal_erSum is: %f" % (numTests, default_erSum/float(numTests), normal_erSum/float(numTests)))
#    print("finished")
