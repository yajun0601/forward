#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 18:08:31 2017

@author: yajun
"""

#import fr_data_set
import tensorflow as tf
from tensorflow.contrib.learn.python.learn.datasets.financial_filled_report import read_data_sets

mnist = read_data_sets('mnist/MNIST_data', one_hot=True)

#mnist = read_data_sets('MNIST_data', one_hot=True) #下载并加载mnist数据
log_dir = 'mnist/logs/ratings_filled_summaries'
                       
#import matplotlib.pyplot as plt                                
#for img0 in mnist.train.images[5:9]:                    
##    img0 = mnist.train.images[0]                                 
#    img0 = img0.reshape(32,40)
#                                
#    fig = plt.figure()  
#    # 第一个子图,按照默认配置  
#    ax = fig.add_subplot(221)  
#    ax.imshow(img0)
#    
#    img1 = img0*255
#    # 第一个子图,按照默认配置  
#    ax = fig.add_subplot(222)  
#    ax.imshow(img1)
#    plt.show() 
    

def variable_summaries(var):
    with tf.name_scope('summaries'):
        mean = tf.reduce_mean(var)
        tf.summary.scalar('mean',mean)
        with tf.name_scope('stddev'):
            stddev = tf.sqrt(tf.reduce_mean(tf.square(var - mean)))
            
        tf.summary.scalar('stddev', stddev)
        tf.summary.scalar('max', tf.reduce_max(var))
        tf.summary.scalar('min', tf.reduce_min(var))
        tf.summary.histogram('histogram', var)
        
sess = tf.InteractiveSession()

with tf.name_scope('input'):
    x = tf.placeholder(tf.float32, shape = [None, 1280]) #输入的数据占位符
    y_ = tf.placeholder(tf.float32, shape = [None, 6]) 

W = tf.Variable(tf.zeros([1280, 6]))
b = tf.Variable(tf.zeros([6]))


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

def conv2d(x, W):
    return tf.nn.conv2d(x, W, strides=[1,1,1,1], padding='SAME')
def max_pool_2x2(x):
    return tf.nn.max_pool(x, ksize=[1,2,2,1], strides=[1,2,2,1], padding = 'SAME')

W_conv1 = weight_variable([5,5,1,32])
'''
The convolution will compute 32 features for each 5x5 patch.Its weight tensor will have a shape of [5,5,1,32].
The first two dimensions are the patch size, the next is the number of input channels, and the last is the number of output channels.
接下来构建网络。整个网络由两个卷积层（包含激活层和池化层），一个全连接层，一个dropout层和一个softmax层组成。 
'''
b_conv1 = bias_variable([32])
# we will also have a bias vector with a component for each output channel.

with tf.name_scope("input_reshape"):
    x_image = tf.reshape(x, [-1,32,40,1]) #转换输入数据shape,以便于用于网络中
    tf.summary.image('input', x_image, 6)

h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + b_conv1)    #第一个卷积层 32*40
h_pool1 = max_pool_2x2(h_conv1)                             #第一个池化层 16*20

# The second layer will have 64 features for each 5x5 patch 
W_conv2 = weight_variable([5,5,32,64])
b_conv2 = bias_variable([64])
h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)    #第二个卷积层
h_pool2 = max_pool_2x2(h_conv2)                             #第二个池化层 8*10

W_fc1 = weight_variable([8*10*64, 1024])
b_fc1 = bias_variable([1024])
h_pool2_flat = tf.reshape(h_pool2, [-1, 8*10*64])            #reshape成向量
h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)  #第一个全连接层

keep_prob = tf.placeholder(tf.float32)
h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)                 #dropout层

# Readout Layer
W_fc2 = weight_variable([1024, 6])
b_fc2 = bias_variable([6])
y_conv = tf.matmul(h_fc1_drop, W_fc2) + b_fc2

# Train and Evaluate the Model
with tf.name_scope("cross_entropy"):
    cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels = y_, logits = y_conv))
    tf.summary.scalar('cross_entropy',cross_entropy)
with tf.name_scope('train'):
    train_step = tf.train.AdamOptimizer(1e-4).minimize(cross_entropy)
    
with tf.name_scope('accuracy'):
    with tf.name_scope('correct_prediction'):
        correct_prediction = tf.equal(tf.argmax(y_conv, 1), tf.argmax(y_, 1))
    with tf.name_scope('accuracy'):
        accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
tf.summary.scalar('accuracy',accuracy)

merged = tf.summary.merge_all()
train_writer = tf.summary.FileWriter(log_dir+'/train', sess.graph)
test_writer = tf.summary.FileWriter(log_dir+'/test')
#sess.run(tf.initialize_all_variables())
tf.global_variables_initializer().run()

def feed_dict(train):
    if train:
        xs,ys = mnist.train.next_batch(50)
        k = 0.5
    else:
        xs,ys = mnist.test.images, mnist.test.labels
        k = 1.0
    return {x:xs, y_:ys, keep_prob: k}

saver = tf.train.Saver()

for i in range(44000):
    if i%150 ==0:
        summary, acc = sess.run([merged, accuracy], feed_dict=feed_dict(False))
        test_writer.add_summary(summary, i)
        print('Accuracy at step %s: %s' % (i, acc))
    else:
        if i%100 == 99:
            run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
            run_metadata = tf.RunMetadata()
            summary, _ = sess.run([merged, train_step], feed_dict=feed_dict(True), options=run_options,run_metadata=run_metadata)
            train_writer.add_run_metadata(run_metadata, 'step%03d' % i)
            train_writer.add_summary(summary, i)
            saver.save(sess,log_dir+'/model.ckpt',i)
            print('Adding run metadata for', i)
        else:
            summary, _ = sess.run([merged, train_step], feed_dict = feed_dict(True))
            train_writer.add_summary(summary, i)
train_writer.close()
test_writer.close()
#
#for i in range(20000):
#    batch = mnist.train.next_batch(50)
#    
#    train_step.run(feed_dict={x:batch[0], y_:batch[1], keep_prob:0.5})
#    if i%100 == 0:
#        train_accuracy = accuracy.eval(feed_dict={x:batch[0], y_:batch[1], keep_prob: 1.0})
#        print("step %d, training accuracy %g"%(i, train_accuracy))
#
#print("test accuracy %g"%accuracy.eval(feed_dict={x:mnist.test.images, y_:mnist.test.labels, keep_prob:1.0}))
#
