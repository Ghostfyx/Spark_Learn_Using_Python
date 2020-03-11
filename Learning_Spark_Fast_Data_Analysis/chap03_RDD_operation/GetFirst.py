#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: GetFirst.py
@time: 2020/2/13 9:50 上午
@desc: Spark RDD的first方法获取RDD中的第一个元素
'''
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("combineKey")
sc = SparkContext(conf=conf)
# 读取本地文件
lines = sc.textFile("/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Learning_Spark_Fast_Data_Analysis/README.md")
print(lines.first())
