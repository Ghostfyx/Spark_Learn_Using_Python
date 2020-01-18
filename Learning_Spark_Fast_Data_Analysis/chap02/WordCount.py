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

conf = SparkConf().setMaster("local").setAppName("Spark-init")
sc = SparkContext(conf=conf)

lines = sc.textFile("README.md")
stringRDD = lines.flatMap(lambda line: line.split(" "))
print(stringRDD)
words = stringRDD.map(lambda word: (word, 1))
print(words.values())
wordCount = words.reduceByKey(lambda x, y: x+y)
print(wordCount.collect())

