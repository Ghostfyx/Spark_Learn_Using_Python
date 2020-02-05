#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: exaple8_1_createSparkContext.py
@time: 2020/2/2 10:27 上午
@desc: 创建SparkConfig
'''
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf()
conf.set("spark.app.name", "example8-1")
conf.set("spark.master", "local[1]")
# 重载默认端口配置
conf.set("spark.ui.port", "36000")
# 使用这个配置对象创建一个SparkContext
sc = SparkContext(conf=conf)

