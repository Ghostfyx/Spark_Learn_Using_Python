#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/10
# @Author  : fanyuexiang
# @Site    :
# @File    : ActionOperate.py
# @Software: PyCharm
# @COMMENTS: RDD中的基本行动操作
from pprint import pprint

from pyspark import SparkConf, SparkContext
import os


def add(x, y):
    """
    向RDD操作中传递函数
    :param x:
    :param y:
    :return:
    """
    return x + y


PYSPARK_PYTHON = "/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("unionRDD")
sc = SparkContext(conf=conf)

RDD = sc.parallelize([1, 3, 4, 5])
'''
基本 RDD 上最常见的行动操作 reduce():
它接收一个函数作为参数，这个 函数要操作两个 RDD 的元素类型的数据并返回一个同样类型的新元素
'''
sumRDD = RDD.reduce(lambda x, y: x + y)
'''
fold:和 reduce() 类似，接收一个与 reduce() 接收的函数签名相同的函数，再加上一个 “初始值”来作为每个分区第一次调用时的结果。
'''
foldRDD = RDD.fold(1, lambda x, y: add(x, y))
