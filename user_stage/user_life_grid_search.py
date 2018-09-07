#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pickle

import numpy as np
import pandas as pd
from sklearn import preprocessing

# 使用交叉验证的方法，把数据集分为训练集合测试集
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.decomposition import PCA
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.naive_bayes import BernoulliNB
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.linear_model import LogisticRegression, SGDClassifier, PassiveAggressiveClassifier, Perceptron, RidgeClassifier
from sklearn.metrics import roc_curve

import matplotlib.pyplot as plt
import seaborn as sns
# SET SEABORN STYLE
sns.set(context='paper', style='white', palette='bone', font='sansserif', font_scale=1.5, color_codes=False, rc=None)

# SET FONT DICTIONARY
font = {
#        'family': 'sansserif',
        'color':  'black',
        'weight': 'normal',
        'variant': 'small-caps',
        'size': 14
        }


# # Data Loading, Exploration and Cleaning

calc_dim_names = ['age_year','gender','userSource', 'reg_period', 'cert_period', 'tie_card_period', 'firstInvestAmount',
                  'withdraw_counts',
                  'withdraw_amount', 'total_charge_amount', 'charge_count', 'max_property', 'total_property',
                  'total_invest_amount',
                  'invest_period', 'total_assign_amount', 'assign_count', 'assign_subject_life_ratio',
                  'total_recovery_amount', 'recovery_count', 'withdraw_ratio', 'assign_ratio']  # all dimension
# todo one-hot encode 'gender'


oneHotEncoder = None
model_filename = './finalized_model.sav'


def load_data(data_path):
    global calc_dim_names

    data_df = pd.read_csv(data_path)
    data_df = data_df[(data_df.gender == data_df.gender)]
    data_df = data_df[(data_df.is_ripe == 1) | (data_df.is_away == 1)]
    # filter no investing action member in current period
    data_df = data_df[
        (data_df.withdraw_amount > 0) | (data_df.total_charge_amount > 0) | (data_df.total_invest_amount > 0) | (
                data_df.total_assign_amount > 0) | (data_df.total_recovery_amount > 0)]
        
    data_df = data_df.fillna(0)
    data_df['financialManager'] = data_df['financialManager'].replace([0, '无'], ['', ''])

    data_df = data_df.sort_values(by="memberId", ascending=False)
#    tmp_df = data_df[calc_dim_names]
#    tmp_df = tmp_df.fillna(0)
#    
#    
#    plt.figure(figsize=(14,12))
#    sns.heatmap(tmp_df.astype(float).corr(),linewidths=0.1,vmax=1.0, 
#                square=True,  linecolor='white', annot=True)
#    plt.show()
#    
    return data_df


def train_LR(train_x, train_y, retrain=False):
    # 选择模型
    global model_filename
    # load the model from disk
    if os.path.exists(model_filename) and retrain:
        print('has last model')
        os.remove(model_filename)
    else:
        return

    # if need train new model, do following
    # 将数据集拆分为训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(
        train_x, train_y, test_size=0.30, random_state=0)
    print('train new model')
    model = LogisticRegression()
    model.fit(x_train, y_train)
    pickle.dump(model, open(model_filename, 'wb'))

    # print model score
    y_pred = model.predict(x_test)
    print("Coefficients:%s, intercept %s" % (model.coef_, model.intercept_))
    print("Residual sum of squares (smaller is better): %.2f" % np.mean((y_pred - y_test) ** 2))
    print('Score: %.2f' % model.score(x_test, y_test))

    print("Classification Report:", classification_report(y_test, y_pred))
    print('Accuracy on Test Data: {0:.3f}'.format(accuracy_score(y_test, y_pred)))
    
    cnf_matrix = confusion_matrix(y_test, y_pred)
    plot_confusion_matrix(cnf_matrix, classes=model.classes_)
    return model


def predict_by_LR(to_predict_x):
    global calc_dim_names
    global model_filename

    # load the model from disk
    model = {}
    if os.path.exists(model_filename):
        print('use current model')
        model = pickle.load(open(model_filename, 'rb'))
    else:
        print('no model')

    # 把数据交给模型预测
    y_pred_array = model.predict(to_predict_x)
    return y_pred_array


def merge_x_and_y(x_df, y_df):
    x_df = x_df[(x_df.memberId.isin(y_df.memberId))]

    y_df = y_df[(y_df.memberId.isin(x_df.memberId))]
    y = y_df[['life_period']]

    return x_df, y


def normalize_ndarr(values):
    normalizer = preprocessing.Normalizer().fit(values)
    normalized_data = normalizer.transform(values)

    return normalized_data


def encode_ndarr_by_label(values):
    labelEncoder = preprocessing.LabelEncoder().fit(values)
    labeled_data = labelEncoder.transform(values)

    return labeled_data


def encode_ndarr_by_onehot(values):
    global oneHotEncoder
    if oneHotEncoder is None:
        oneHotEncoder = preprocessing.OneHotEncoder().fit(values.reshape(-1, 1))
    oneHoted_data = oneHotEncoder.transform(values.reshape(-1, 1))

    return oneHoted_data.toarray()


def preprocess_df(df, dims):
    global calc_dim_names
    dim_series = df[dims].values

    label_encoded_ndarr = encode_ndarr_by_label(dim_series)
    onehot_encoded_ndarr = encode_ndarr_by_onehot(label_encoded_ndarr)
    total_calc_encoded_x_data = np.concatenate((df[calc_dim_names].values, onehot_encoded_ndarr),
                                               axis=1)  # 较x原始数据少了一些不参与计算的列，多了一些由one-hot得到的更多列

    normalized_x_data = normalize_ndarr(total_calc_encoded_x_data)

    return normalized_x_data

# USED TO PLOT A CONFUSION MATRIX TO CHECK HOW THE CLASSIFICATION MODEL WORKS ON THE TEST SET
import itertools
def plot_confusion_matrix(cm, classes, title='Confusion matrix', cmap=plt.cm.bone):
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title, fontdict=font)
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes)
    plt.yticks(tick_marks, classes)
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, cm[i, j],
                 horizontalalignment="center",
                 color="black" if cm[i, j] > thresh else "white",
                fontdict=font)
    plt.ylabel('True label', fontdict=font, fontsize=10)
    plt.xlabel('Predicted label', fontdict=font, fontsize=10)
    plt.show()

def grid_search(x,y):
    
    
    # TRAIN AND TEST SPLIT WITH TEST SIZE OF 20%
    data_train,data_test,label_train,label_test = train_test_split(x.values,y.values.ravel(),test_size = 0.25)
    # SET PARAMETER SPACE FOR GRID SEARCH ON PCA COMPONENTS
    parameters = {
    'pca__n_components': [2, 4, 8, 10, 20]
    }
        
    # In[43]:
        
    # LIST OF BINARY CLASSIFIERS
    models = [KNeighborsClassifier(),
              LogisticRegression(),
              DecisionTreeClassifier(),
              SVC(),
              RandomForestClassifier(),
              ExtraTreesClassifier(),
#              SGDClassifier(),
#              Perceptron(),
#              PassiveAggressiveClassifier(),
#              RidgeClassifier(),
#              BernoulliNB(),
              ]
    for model in models:
        pipeline = Pipeline([
            ('ss', StandardScaler()),
            ('pca', PCA(random_state=42)),
            ('clf', model), 
        ])
        grid_search = GridSearchCV(pipeline, parameters, verbose=1, cv=5)
        grid_search.fit(data_train, label_train)
        print("Model", model)
        print("Grid Search Best Score:", grid_search.best_score_)
        print("Grid Search Best Params:", grid_search.best_params_)
        
        predictions = grid_search.best_estimator_.predict(data_test)
        print("Classification Report:\n", classification_report(label_test, predictions))
        print('Accuracy on Test Data: {0:.3f}'.format(accuracy_score(label_test, predictions)))
    
        cnf_matrix = confusion_matrix(label_test, predictions)
        plot_confusion_matrix(cnf_matrix, classes=grid_search.best_estimator_.classes_)


if __name__ == '__main__':
    to_train_data_path = './life_period_start_60_end_30_table.csv'
    to_train_data_path_2 = './life_period_start_90_end_60_table.csv'
    to_predict_data_path = './life_period_start_30_end_1_table.csv'

    to_train_df = load_data(to_train_data_path)
    to_train_df2 = load_data(to_train_data_path_2)
    to_predict_df = load_data(to_predict_data_path)

    # 训练集抽样代码
    # train_away_sample_df = to_train_df[(to_train_df.life_period > 0)].sample(n=1500, axis=0)
    # to_train_df = to_train_df[(to_train_df.life_period < 1)].append(train_away_sample_df)
    # to_train_df = to_train_df.sort_values(by="memberId", ascending=False)

    # merge train2_x-train1_y, train1_x-predict_y
    x1, y1 = merge_x_and_y(to_train_df, to_predict_df)
    x2, y2 = merge_x_and_y(to_train_df2, to_train_df)

    # x = x1
    # y = y1
    x = x1.append(x2)
    y = y1.append(y2)
    
    x = x[calc_dim_names]
    grid_search(x,y)

    preprocess_dim_names = ['financialManager']
    # training pre-process label-encode, one-hot-encode, normalize
    preprocessed_x_ndarr = preprocess_df(x, preprocess_dim_names)


    # do train model
    model = train_LR(preprocessed_x_ndarr, y.values.ravel(), retrain=True)  # 训练模型

    # to predict which ripe member is going to be away
    ripe_member_df = to_predict_df[(to_predict_df.life_period < 1)]
    preprocessed_ripe_member_x_ndarr = preprocess_df(ripe_member_df, preprocess_dim_names)

    ripe_member_predicted_y_array = predict_by_LR(preprocessed_ripe_member_x_ndarr)

    predicted_result_df = ripe_member_df.copy(deep=True)
    predicted_result_df['life_period'] = ripe_member_predicted_y_array

    # print predict score
    print("Residual sum of squares: %.2f" % np.mean((ripe_member_df['life_period'].values - ripe_member_predicted_y_array) ** 2))
    print("classification report:")
    print(classification_report(ripe_member_df['life_period'].values, ripe_member_predicted_y_array))

    # output predict result
    predicted_result_df.to_csv('./predict_result_table.csv', sep=',', encoding='utf-8')

    # unused api
    # return pd.concat([x_data_df, y_pred_df], axis=1)
    # predict_result_df = pd.DataFrame(predict_x, columns=calc_dimension_names)
    # predict_result_df['life_period'] = y_pred_df
    # data_df = pd.read_excel(data_file_path, sheet_name='Sheet1')
