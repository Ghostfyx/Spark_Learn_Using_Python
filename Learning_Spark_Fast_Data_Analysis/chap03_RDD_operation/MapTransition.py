#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/10
# @Author  : fanyuexiang
# @Site    : 
# @File    : MapTransition.py
# @Software: PyCharm
# @COMMENTS: 使用map转换操作用于 RDD 中的每个元素
import math
from pprint import pprint

from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("unionRDD")
sc = SparkContext(conf=conf)
nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: math.sqrt(x))

