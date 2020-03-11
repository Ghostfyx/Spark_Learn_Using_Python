#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/10
# @Author  : fanyuexiang
# @Site    : 
# @File    : AggregateOperate.py
# @Software: PyCharm
# @COMMENTS:  aggregate() 来计算 RDD 的平均值,来代替 map() 后面接 fold() 的方式
from pprint import pprint

from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("AggregateOperate")
sc = SparkContext(conf=conf)
RDD = sc.parallelize([1, 3, 4, 5])
'''
aggregate：聚合RDD所有分区的每个元素（先对每个分区的元素做聚集，然后对所有分区的结果做聚集）
入参详解：
        （1）zeroValue：聚集函数的初始值；
        （2）seqOp：an operator used to accumulate results within a partition；
        （3）combOp：an associative operator used to combine results from different partitions
    PS：seqOp与combOp函数C{op(t1, t2)}用于修改t1，并返回t1。 
'''
sumCount = RDD.aggregate(zeroValue=(0, 0),
                         seqOp=(lambda acc, value: (acc[0]+value, acc[1]+1)),
                         combOp=(lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
                         )
average = sumCount[0]/sumCount[1]
