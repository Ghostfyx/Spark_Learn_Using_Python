#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: UnionRDD.py
@time: 2020/2/13 10:14 上午
@desc: 使用union转化操作
'''
from pprint import pprint

from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("unionRDD")
sc = SparkContext(conf=conf)
lines = sc.textFile("file:///Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Learning_Spark_Fast_Data_Analysis/data/log.txt")
warningRDD = lines.filter(lambda line: "WARNING" in line)
errorRDD = lines.filter(lambda line: "ERROR" in line)
badLinesRDD = warningRDD.union(errorRDD)
# collect一般只会用于本地调试，不能在生产环境中使用
pprint(badLinesRDD.collect())
# 使用count行动操作计算数量
pprint(badLinesRDD.count())
