#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: DStreamingOutput.py
@file: DStreamingOutput.py
@time: 2020/2/23 4:17 下午
@desc: Spark DStreaming 输出操作，保存为Hadoop支持的文件格式或者输出在控制台
'''
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
import os

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setAppName("streaming_output").setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sparkContext=sc, batchDuration=10)
lines = ssc.socketTextStream(hostname="localhost", port=7777)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
count = pairs.reduceByKey(lambda x, y: x+y)
# python API暂时只支持保存为text文件
count.saveAsTextFiles()
count.foreachRDD(lambda rdd: rdd)

