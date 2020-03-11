#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: WordCount.py
@time: 2020/1/19 6:20 上午
@desc: 大数据入门-单词统计
'''
from pyspark import SparkConf, SparkContext
import platform

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
if platform.system() == 'Windows':
    PYSPARK_PYTHON = "/d/python3/python"

conf = SparkConf().setMaster("local").setAppName("word-count")
sc = SparkContext(conf=conf)

lines = sc.textFile("../README.md")
stringRDD = lines.flatMap(lambda line: line.split(" "))
print(stringRDD)
words = stringRDD.map(lambda word: (word, 1))
print(words.values())
wordCount = words.reduceByKey(lambda x, y: x+y)
wordCount.collect()
