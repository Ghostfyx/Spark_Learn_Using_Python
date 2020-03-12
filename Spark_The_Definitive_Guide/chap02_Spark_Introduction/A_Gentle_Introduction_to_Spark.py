#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/12
# @Author  : fanyuexiang
# @Site    : 
# @File    : A_Gentle_Introduction_to_Spark.py
# @Software: PyCharm
# @COMMENTS: 2.9. 一个相对完整的例子
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

PYSPARK_PYTHON = "/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark = SparkSession.builder.appName("simpleExample").getOrCreate()
# 设置shuffle分区数量
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("/data/flight-data/csv/*.csv")
flightData2015_sorted = flightData2015.sort("count")
# 查看Spark对数据的执行计划
flightData2015.sort("count").explain()
flightData2015_sorted.take(2)
# 将DataFrame转换为临时视图
flightData2015.createOrReplaceTempView("flight_data_2015")
sqlWay = spark.sql("""
    select DEST_COUNTRY_NAME, count(1)
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
""")
dataFramesWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
sqlWay.explain()
dataFramesWay.explain()
maxSql = spark.sql(""" 
    select DEST_COUNTRY_NAME, sum(count) as destination_total 
    FROM flight_data_2015 
    GROUP BY DEST_COUNTRY_NAME 
    ORDER BY sum(count) DESC 
    LIMIT 5
""")
maxDataFrame = flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "destination_total")\
    .sort(desc("destination_total")).limit(5)
