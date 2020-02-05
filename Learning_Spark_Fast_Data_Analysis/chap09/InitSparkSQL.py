#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: InitSparkSQL.py
@time: 2020/2/5 10:37 上午
@desc: 初始化SparkSql
'''
from pyspark.sql import HiveContext, Row
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf()
conf.set("spark.app.name", "init-sparkSql")
conf.set("spark.master", "local[1]")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
print(hiveContext)
