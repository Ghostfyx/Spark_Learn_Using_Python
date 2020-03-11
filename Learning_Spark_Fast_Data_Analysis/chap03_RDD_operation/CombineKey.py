#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: CombineKey.py
@time: 2020/1/31 6:11 下午
@desc: Spark RDD 键值对转换操作 combineByKey
'''
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf().setMaster("local").setAppName("combineKey")
sc = SparkContext(conf=conf)
rdd1 = sc.parallelize([("A", 1), ("A", 2), ("B", 2), ("C", 3), ("B", 1)], 3)
rdd2 = rdd1.combineByKey(
    # 将原RDD中的value转换为value_
    createCombiner=(lambda v: str(v) + '_'),
    # 合并值函数
    mergeValue=(lambda c, v: str(c)+'@'+str(v)),
    # 合并器函数
    mergeCombiners=(lambda c1, c2: str(c1)+'$'+str(c2)),
    numPartitions=3
)
print(rdd2.collect())
