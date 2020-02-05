#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: ReadDataFromJson.py
@time: 2020/2/5 10:52 上午
@desc:  使用Spark SQL查询json数据
'''
from pyspark.sql import HiveContext, Row
from pyspark import SparkConf, SparkContext
import os
PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

conf = SparkConf()
conf.set("spark.app.name", "init-sparkSql")
conf.set("spark.master", "local[1]")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
dataFrame = hiveContext.read.json("test.json")
dataFrame.show()
dataFrame.printSchema()
# 注册临时表
dataFrame.registerTempTable("panda")
sqlDF = hiveContext.sql("select * from panda")
sqlDF.show()
