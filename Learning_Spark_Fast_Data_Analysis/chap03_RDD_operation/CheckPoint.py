#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: CheckPoint.py
@time: 2020/1/31 8:48 下午
@desc: Spark checkPoint操作设置检查点
'''
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("combineKey")
sc = SparkContext(conf=conf)
# 设置checkPoint文件保存路径
sc.setCheckpointDir("")
rdd1 = sc.parallelize([("A", 1), ("A", 2), ("B", 2), ("C", 3), ("B", 1)])
rdd1.checkpoint()
rdd1.collect()
