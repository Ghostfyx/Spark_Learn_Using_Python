#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/10
# @Author  : fanyuexiang
# @Site    : 
# @File    : CreateRDD.py
# @Software: PyCharm
# @COMMENTS: 创建RDD的几种方法
import os
import platform
from pprint import pprint

from pyspark import SparkContext, SparkConf

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
if platform.system() == 'Windows':
    PYSPARK_PYTHON = "/d/python3/python"

os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("create_RDD")
sc = SparkContext(conf=conf).setLogLevel('INFO')

'''
在Spark中创建RDD中的两种方式：
    1. 从外部数据集中；
    2. 在驱动器程序中对一个集合进行并行化；
'''
parallelizeRDD = sc.parallelize(["pandas", "i like pandas", "chengdu pandas", "kongfu pandas"])
lines = sc.textFile("hdfs://master:9000/spark_learn/data/word_count.txt")




