#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: foldKey.py
@time: 2020/1/31 7:38 下午
@desc: Spark RDD 键值对转换操作 foldByKey
'''
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("combineKey")
sc = SparkContext(conf=conf)
rdd1 = sc.parallelize([("A", 1), ("A", 2), ("B", 2), ("C", 3), ("B", 1)], 3)
rdd2 = rdd1.foldByKey(
    zeroValue=2,
    func=(lambda v1, v2: v1+v2)
)
print(rdd2.collect())
rdd3 = rdd1.foldByKey(
    zeroValue=0,
    func=(lambda v1, v2: v1*v2)
)
print(rdd3.collect())
