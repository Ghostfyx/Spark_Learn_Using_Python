#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: Cogroup.py
@time: 2020/1/31 8:12 下午
@desc: Spark RDD 键值操作 cogroup操作
'''
from pprint import pprint

from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("combineKey")
sc = SparkContext(conf=conf)
rdd1 = sc.parallelize([("A", 1), ("A", 2), ("B", 2), ("C", 3), ("B", 1)])
rdd2 = sc.parallelize([("A", 1), ("A", 2), ("B", 2), ("C", 3), ("E", 1)])
rdd3 = sc.parallelize([("G", 1), ("H", 2), ("F", 1)])
rdd4 = rdd1.cogroup(
    rdd2
)
pprint(rdd4.collect())

