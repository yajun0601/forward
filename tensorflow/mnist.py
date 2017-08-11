#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 11:00:17 2017

@author: yajun
"""

import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data
'''
mnist的卷积神经网络例子和上一篇博文中的神经网络例子大部分是相同的。但是CNN层数要多一些，网络模型需要自己来构建。

程序比较复杂，我就分成几个部分来叙述。

首先，下载并加载数据：
'''
mnist = input_data.read_data_sets('MNIST_data', one_hot=True) #下载并加载mnist数据

import matplotlib.pyplot as plt                                
for img0 in mnist.train.images[5:9]:                    
#    img0 = mnist.train.images[0]                                 
    img0 = img0.reshape(28,28)
                                
    fig = plt.figure()  
    # 第一个子图,按照默认配置  
    ax = fig.add_subplot(221)  
    ax.imshow(img0)
    
    img1 = img0*255
    # 第一个子图,按照默认配置  
    ax = fig.add_subplot(222)  
    ax.imshow(img1)
    plt.show() 
                                     
sess = tf.InteractiveSession()

x = tf.placeholder(tf.float32, shape = [None, 784]) #输入的数据占位符
y_ = tf.placeholder(tf.float32, shape = [None, 10]) 
W = tf.Variable(tf.zeros([784, 10]))
b = tf.Variable(tf.zeros([10]))

#sess.run(tf.global_variables_initializer())
y = tf.matmul(x,W) + b

#cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels = y_, logits=y))
#定义四个函数，分别用于初始化权值W，初始化偏置项b, 构建卷积层和构建池化层。 
def weight_variable(shape):
    initial = tf.truncated_normal(shape, stddev = 0.1)
    return tf.Variable(initial)

def bias_variable(shape):
    initial = tf.constant(0.1, shape= shape)
    return tf.Variable(initial)


W_conv1 = weight_variable([5,5,1,32])
'''
5,5 代表卷积核的尺寸 5*5
1   代表有多少个 channel，由于是灰度图像，所以这里是 1 
32  代表这里卷积核的数量，也就是这个卷积层会提取多少类的特征
The convolution will compute 32 features for each 5x5 patch.Its weight tensor will have a shape of [5,5,1,32].
The first two dimensions are the patch size, the next is the number of input channels, and the last is the number of output channels.
接下来构建网络。整个网络由两个卷积层（包含激活层和池化层），一个全连接层，一个dropout层和一个softmax层组成。 
'''
def conv2d(x, W):
    return tf.nn.conv2d(x, W, strides=[1,1,1,1], padding='SAME')
def max_pool_2x2(x):
    return tf.nn.max_pool(x, ksize=[1,2,2,1], strides=[1,2,2,1], padding = 'SAME')


b_conv1 = bias_variable([32])
# we will also have a bias vector with a component for each output channel.

x_image = tf.reshape(x, [-1,28,28,1]) #转换输入数据shape,以便于用于网络中

h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + b_conv1)    #第一个卷积层
h_pool1 = max_pool_2x2(h_conv1)                             #第一个池化层

# The second layer will have 64 features for each 5x5 patch 
W_conv2 = weight_variable([5,5,32,64])
b_conv2 = bias_variable([64])
h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)    #第二个卷积层
h_pool2 = max_pool_2x2(h_conv2)                             #第二个池化层

W_fc1 = weight_variable([7*7*64, 1024])
b_fc1 = bias_variable([1024])
h_pool2_flat = tf.reshape(h_pool2, [-1, 7*7*64])            #reshape成向量
h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)  #第一个全连接层

keep_prob = tf.placeholder(tf.float32)
h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)                 #dropout层

# Readout Layer
W_fc2 = weight_variable([1024, 10])
b_fc2 = bias_variable([10])
y_conv = tf.matmul(h_fc1_drop, W_fc2) + b_fc2

# Train and Evaluate the Model
cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels = y_, logits = y_conv))
train_step = tf.train.AdamOptimizer(1e-4).minimize(cross_entropy)
correct_prediction = tf.equal(tf.argmax(y_conv, 1), tf.argmax(y_, 1))
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
sess.run(tf.initialize_all_variables())

for i in range(20000):
    batch = mnist.train.next_batch(50)
    train_step.run(feed_dict={x:batch[0], y_:batch[1], keep_prob:0.5})
    if i%100 == 0:
        train_accuracy = accuracy.eval(feed_dict={x:batch[0], y_:batch[1], keep_prob: 1.0})
        print("step %d, training accuracy %g"%(i, train_accuracy))

print("test accuracy %g"%accuracy.eval(feed_dict={x:mnist.test.images, y_:mnist.test.labels, keep_prob:1.0}))

