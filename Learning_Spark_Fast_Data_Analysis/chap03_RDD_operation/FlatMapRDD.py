#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: FlatMapRDD.py
@time: 2020/2/13 10:53 上午
@desc: 使用map和flatMap操作
'''
from pprint import pprint

from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("unionRDD")
sc = SparkContext(conf=conf)
lines = sc.textFile("file:///Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Learning_Spark_Fast_Data_Analysis/data/log.txt")
'''
flatMap()：对输入元素生成多个输出元素。
'''
flatWords = lines.flatMap(lambda line: line.split(' '))
# pprint(flatWords.collect())
words = lines.map(lambda line: line.split(' '))
pprint(words.collect())
