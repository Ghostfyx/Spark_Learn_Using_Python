#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: Filter.py
@time: 2020/2/13 9:28 上午
@desc: 使用RDD的filter操作
'''
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("combineKey")
sc = SparkContext(conf=conf)
# 读取本地文件
lines = sc.textFile("/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Learning_Spark_Fast_Data_Analysis/README.md")
filter_word = 'Python'
lines.filter(lambda line: filter_word in line).collect()
