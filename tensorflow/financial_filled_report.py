# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""Functions for downloading and reading MNIST data."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gzip
from sklearn.cross_validation import train_test_split
import numpy as np
import numpy
from six.moves import xrange  # pylint: disable=redefined-builtin

from tensorflow.contrib.learn.python.learn.datasets import base
from tensorflow.python.framework import dtypes

SOURCE_URL = 'http://yann.lecun.com/exdb/mnist/'


def _read32(bytestream):
  dt = numpy.dtype(numpy.uint32).newbyteorder('>')
  return numpy.frombuffer(bytestream.read(4), dtype=dt)[0]


def extract_images(f):
  """Extract the images into a 4D uint8 numpy array [index, y, x, depth].

  Args:
    f: A file object that can be passed into a gzip reader.

  Returns:
    data: A 4D uint8 numpy array [index, y, x, depth].

  Raises:
    ValueError: If the bytestream does not start with 2051.

  """
  print('Extracting', f.name)
  with gzip.GzipFile(fileobj=f) as bytestream:
    magic = _read32(bytestream)
    if magic != 2051:
      raise ValueError('Invalid magic number %d in MNIST image file: %s' %
                       (magic, f.name))
    num_images = _read32(bytestream)
    rows = _read32(bytestream)
    cols = _read32(bytestream)
    buf = bytestream.read(rows * cols * num_images)
    data = numpy.frombuffer(buf, dtype=numpy.uint8)
    data = data.reshape(num_images, rows, cols, 1)
    return data


def dense_to_one_hot(labels_dense, num_classes):
  """Convert class labels from scalars to one-hot vectors."""
#  print(labels_dense)
#  print(labels_dense.shape)
  num_labels = labels_dense.shape[0]
  index_offset = numpy.arange(num_labels) * num_classes
  labels_one_hot = numpy.zeros((num_labels, num_classes))
  labels_one_hot.flat[index_offset + labels_dense.ravel()] = 1
  return labels_one_hot


def extract_labels(f, one_hot=False, num_classes=10):
  """Extract the labels into a 1D uint8 numpy array [index].

  Args:
    f: A file object that can be passed into a gzip reader.
    one_hot: Does one hot encoding for the result.
    num_classes: Number of classes for the one hot encoding.

  Returns:
    labels: a 1D uint8 numpy array.

  Raises:
    ValueError: If the bystream doesn't start with 2049.
  """
  print('Extracting', f.name)
  with gzip.GzipFile(fileobj=f) as bytestream:
    magic = _read32(bytestream)
    if magic != 2049:
      raise ValueError('Invalid magic number %d in MNIST label file: %s' %
                       (magic, f.name))
    num_items = _read32(bytestream)
    buf = bytestream.read(num_items)
    labels = numpy.frombuffer(buf, dtype=numpy.uint8)
    if one_hot:
      return dense_to_one_hot(labels, num_classes)
    return labels


class DataSet(object):

  def __init__(self,
               images,
               labels,
               fake_data=False,
               one_hot=False,
               dtype=dtypes.float32,
               reshape=True):
    """Construct a DataSet.
    one_hot arg is used only if fake_data is true.  `dtype` can be either
    `uint8` to leave the input as `[0, 255]`, or `float32` to rescale into
    `[0, 1]`.
    """
    dtype = dtypes.as_dtype(dtype).base_dtype
    if dtype not in (dtypes.uint8, dtypes.float32):
      raise TypeError('Invalid image dtype %r, expected uint8 or float32' %
                      dtype)
    if fake_data:
      self._num_examples = 10000
      self.one_hot = one_hot
    else:
      assert images.shape[0] == labels.shape[0], (
          'images.shape: %s labels.shape: %s' % (images.shape, labels.shape))
      self._num_examples = images.shape[0]

      # Convert shape from [num examples, rows, columns, depth]
      # to [num examples, rows*columns] (assuming depth == 1)
      if reshape:
        assert images.shape[3] == 1
        images = images.reshape(images.shape[0],
                                images.shape[1] * images.shape[2])
      if dtype == dtypes.float32:
        # Convert from [0, 255] -> [0.0, 1.0].
        images = images.astype(numpy.float32)
        images = numpy.multiply(images, 1.0 / 255.0)
    self._images = images
    self._labels = labels
    self._epochs_completed = 0
    self._index_in_epoch = 0

  @property
  def images(self):
    return self._images

  @property
  def labels(self):
    return self._labels

  @property
  def num_examples(self):
    return self._num_examples

  @property
  def epochs_completed(self):
    return self._epochs_completed

  def next_batch(self, batch_size, fake_data=False, shuffle=True):
    """Return the next `batch_size` examples from this data set."""
    if fake_data:
      fake_image = [1] * 784
      if self.one_hot:
        fake_label = [1] + [0] * 9
      else:
        fake_label = 0
      return [fake_image for _ in xrange(batch_size)], [
          fake_label for _ in xrange(batch_size)
      ]
    start = self._index_in_epoch
    # Shuffle for the first epoch
    if self._epochs_completed == 0 and start == 0 and shuffle:
      perm0 = numpy.arange(self._num_examples)
      numpy.random.shuffle(perm0)
      self._images = self.images[perm0]
      self._labels = self.labels[perm0]
    # Go to the next epoch
    if start + batch_size > self._num_examples:
      # Finished epoch
      self._epochs_completed += 1
      # Get the rest examples in this epoch
      rest_num_examples = self._num_examples - start
      images_rest_part = self._images[start:self._num_examples]
      labels_rest_part = self._labels[start:self._num_examples]
      # Shuffle the data
      if shuffle:
        perm = numpy.arange(self._num_examples)
        numpy.random.shuffle(perm)
        self._images = self.images[perm]
        self._labels = self.labels[perm]
      # Start next epoch
      start = 0
      self._index_in_epoch = batch_size - rest_num_examples
      end = self._index_in_epoch
      images_new_part = self._images[start:end]
      labels_new_part = self._labels[start:end]
      return numpy.concatenate((images_rest_part, images_new_part), axis=0) , numpy.concatenate((labels_rest_part, labels_new_part), axis=0)
    else:
      self._index_in_epoch += batch_size
      end = self._index_in_epoch
      return self._images[start:end], self._labels[start:end]

from pymongo import MongoClient
import pandas as pd
def get_rating():
    client = MongoClient("mongodb://192.168.10.60:27017/")
    db = client.bonds
    query = db.rate_sample_set.find({},{'_id':0})
    all_rating = pd.DataFrame(list(query))
     
    print(all_rating.groupby('LATESTISSURERCREDITRATING2').size().sort_values())
    sorted_rating = all_rating.sort_values(['code','rptDate'], ascending=False)
    sorted_rating = sorted_rating.reset_index(drop=True)
    
    # fill and drop 'None' value
    for j in range(int(len(sorted_rating)/14)):
        for i in range(14):
            index = 14*j + i
            if sorted_rating.iloc[index]['LATESTISSURERCREDITRATING2'] is None:
                if i != 0:
                    sorted_rating.iloc[index]['LATESTISSURERCREDITRATING2'] = sorted_rating.iloc[index-1]['LATESTISSURERCREDITRATING2']
    rating = sorted_rating.dropna()    
    # print the count after drop null
#    print(rating.groupby('rptDate').size().sort_values())
    
    ret_rating = rating[rating['rptDate'] == '20160630']
    ret_rating['rptDate'] = ret_rating['rptDate'].replace('20160630','20151231')
    
    rating_map = pd.DataFrame(list(range(6)), index=['AAA','AA+','AA','AA-','A','BC'])
    
    for r in rating_map.index:
#        print(r)
        ret_rating['LATESTISSURERCREDITRATING2'] = ret_rating['LATESTISSURERCREDITRATING2'].replace(r,rating_map.T[r].values[0])
    client.close()
    return ret_rating

def map_rate(x):
#    rates=['AAA','AA+','AA','AA-','A+','A','A-','BBB+','BBB','BBB-','BB+','BB','BB-','B+','B','B-','CCC+','CCC','CCC-','CC+','CC','CC-','DDD']
    rates=['AAA','AA+','AA','AA-','A','BC']

    ret = 0
    for i in rates:
        if x == i:
            ret = len(rates) - rates.index(i)
    return ret

def read_data_sets(train_dir,
                   fake_data=False,
                   one_hot=False,
                   dtype=dtypes.float32,
                   reshape=True,
                   validation_size=5000):
  if fake_data:
    def fake():
      return DataSet([], [], fake_data=True, one_hot=one_hot, dtype=dtype)

    train = fake()
    validation = fake()
    test = fake()
    return base.Datasets(train=train, validation=validation, test=test)

  
  client = MongoClient("mongodb://127.0.0.1:27017/")
  db = client.bonds
  
  qv = db.PROCESSED_SET_2.find({'rptDate':'20161231'},{'_id':0,'rptDate':0,'LATESTISSURERCREDITRATING2':0})#.limit(100) 
  financial_report = pd.DataFrame(list(qv)) # 获取技术性违约样本，从 2015年的财报中选择
  ratings = get_rating()
  ratios = financial_report.merge(ratings, on='code')
  rating = ratios.pop('LATESTISSURERCREDITRATING2')
  labels = dense_to_one_hot(rating,6)
  data = ratios.drop(['code','rptDate'], axis=1)
  zz = numpy.zeros([len(data),1])
  for i in range(1280-data.shape[1]):
      data.insert(0,i,zz)
      
  samples = data.values.reshape(len(data),32,40,1)
  
  train_images, test_images, train_labels, test_labels = train_test_split(samples, labels, random_state=1)


  train = DataSet(train_images, train_labels, dtype=dtype, reshape=reshape)
  test = DataSet(test_images, test_labels, dtype=dtype, reshape=reshape)
#  validation = test
#  validation = DataSet(validation_images,validation_labels,dtype=dtype,reshape=reshape)
  client.close()
  return base.Datasets(train=train, validation=test, test=test)


def load_mnist(train_dir='MNIST-data'):
  return read_data_sets(train_dir)


    