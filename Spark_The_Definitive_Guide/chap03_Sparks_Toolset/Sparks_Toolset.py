#!/usr/bin/env python
# encoding: utf-8
"""
@author: fanyuexiang
@software: pycharm
@file: Sparks_Toolset.py
@time: 2020/5/13 11:07 下午
@desc: Spark权威指南第三章 Spark工具集简介
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

PYSPARK_PYTHON = "/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
spark = SparkSession.builder.appName("chap03").master("local[*]").getOrCreate()
'''
python中不能使用类型安全API Dataset，因为Python是动态语言
Dataset<T> T是静态类型，可用于Java和Scala中

结构化流处理：结构化流处理是用于数据流处理的高级API，可以减少延迟并允许增量处理；
    最重要的是可以按照传统批处理作业的模式进行设计，然后转为流式作业
    以下示例按天读取交易数据，是以天为时间窗口的时间序列数据
'''
# 静态DataFrame版本
staticDataFrame = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(
    "/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Spark_The_Definitive_Guide/data/retail-data/by-day/*.csv")
# 指定时间窗口为天，每个窗口包含一天的数据
window_df = staticDataFrame.selectExpr("CustomerId",
                                       "(UnitPrice * Quantity) as total_cost",
                                       "InvoiceDate") \
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).sum("total_cost")
# 修改默认分区数，默认为200
spark.conf.set("spark.sql.shuffle.partitions", "5")
retail_schema = staticDataFrame.schema
# 执行结构化流的schema；每次从文件中读取一条数据，模拟流式数据；
streamingDataFrame = staticDataFrame = spark.readStream.schema(retail_schema) \
    .option("maxFilesPerTrigger", 1) \
    .format("csv") \
    .option("header", "true") \
    .load(
    "/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Spark_The_Definitive_Guide/data/retail-data/by-day/*.csv")
print(staticDataFrame.isStreaming)
'''
进行与静态DataFrame相同的计算逻辑
流数据的计算结果将被换存在一个内存的数据表里，在每次触发器触发后更新内存缓存
本例中：maxFilesPerTrigger是触发器，Spark将基于新读入的文件更新内存数据表的内容
'''
window_streaming_df = streamingDataFrame.selectExpr("CustomerId",
                                       "(UnitPrice * Quantity) as total_cost",
                                       "InvoiceDate") \
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).sum("total_cost")
# 打印到控制台
window_streaming_df.writeStream.format("console")\
    .queryName("customer_purchases").outputMode("complete").start()
# 存入内存中
# window_streaming_df.writeStream.format("memory")\
#     .queryName("customer_purchases").outputMode("complete").start()

'''
Spark Mllib 提供了一系列机器学习算法与特征工程相关的类库
'''

