#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: LogStreaming.py
@time: 2020/2/23 3:15 下午
@desc: 使用Spark Streaming 处理日志信息，日志脚本见log.sh
'''
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
import os

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setAppName("log_streaming").setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sparkContext=sc, batchDuration=2)
lines = ssc.socketTextStream(hostname="localhost", port=7777)
error_lines = lines.filter(lambda line: "error" in line)
error_lines.pprint()
# 启动流计算环境StreamingContext,并等待他计算完成
ssc.start()
ssc.awaitTermination()


