#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: CreateSparkContext.py
@time: 2020/1/19 5:46 上午
@desc: 连接Spark，创建SparkContext
'''
from pyspark import SparkConf, SparkContext
import os
import platform

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
if platform.system()=='Windows':
    PYSPARK_PYTHON = "/d/python3/python"

os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("Spark-init")
sc = SparkContext(conf=conf)
print(sc)
