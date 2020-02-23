#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: StreamingInit.py
@time: 2020/2/23 2:41 下午
@desc: 使用Python 初始化Spark Streaming，实现wordCount功能
'''
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
import os

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf()
conf.set("spark.app.name", "init-streaming")
conf.set("spark.master", "local[2]")
sc = SparkContext(conf=conf)
streamingSc = StreamingContext(sparkContext=sc, batchDuration=1)
lines = streamingSc.socketTextStream(hostname="localhost", port=7777)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
count = pairs.reduceByKey(lambda x, y: x+y)
count.pprint()
streamingSc.start()
streamingSc.awaitTermination()

