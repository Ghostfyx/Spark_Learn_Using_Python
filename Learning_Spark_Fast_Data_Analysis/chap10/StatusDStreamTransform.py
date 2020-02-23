#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: StatusDStreamTransform.py
@time: 2020/2/23 3:45 下午
@desc: 有状态DStream转换
'''
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
import os

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setAppName("log_streaming").setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sparkContext=sc, batchDuration=10)
# 有状态DStream转换为了防止计算逻辑DAG图中某个节点计算失效，导致从头开始计算，需要设置检查点
ssc.checkpoint("/tmp")
# 窗口操作：结果返回DStream的每个RDD会包含多个批次数据
accessLogDStream = ssc.socketTextStream(hostname="localhost", port=7777)
# 窗口时长为30s，即三个批次；滑动步长为10s，即一个批次
windows = accessLogDStream.window(windowDuration=30, slideDuration=10)
# reduceByKeyAndWindow对窗口进行高效操作，通过考虑进入窗口的元素和离开窗口的数据，对Spark进行增量计算，如下：对进入窗口的数据进行加和计算，对离开窗口的数据进行减法计算
windows_by_key = accessLogDStream.reduceByKeyAndWindow(func=lambda x, y: x+y, invFunc=lambda x, y: x-y, windowDuration=30, slideDuration=10)
# 返回窗口中元素的个数
window_count_DStream = accessLogDStream.countByWindow(30, 10)
# 返回窗口中包含的每个值的个数
window_value_count = accessLogDStream.countByValueAndWindow(30, 10)
windowsCount = windows.count()
windowsCount.pprint()
ssc.start()
ssc.awaitTermination()
