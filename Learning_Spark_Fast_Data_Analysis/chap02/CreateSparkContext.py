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

conf = SparkConf().setMaster("local").setAppName("Spark-init")
sc = SparkContext(conf=conf)
print(sc)
