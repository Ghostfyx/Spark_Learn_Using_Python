#!/usr/bin/env python
# encoding: utf-8
"""
@author: fanyuexiang
@software: pycharm
@file: Structured_APIs.py
@time: 2020/5/14 10:22 下午
@desc: Spark结构化API概述
"""
import os

from pyspark.sql import SparkSession

PYSPARK_PYTHON = "/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark = SparkSession.builder.master("local[*]").appName("chap04").getOrCreate()
# Spark的行和列的概念,Spark DataFrame是行和列组成的分布式表格
row = spark.range(2).collect()
"""
[Row(id=0), Row(id=1)]
"""
print(row)
column = spark.range(2).toDF("number")
'''
+------+
|number|
+------+
|     0|
|     1|
+------+
'''
column.show()

