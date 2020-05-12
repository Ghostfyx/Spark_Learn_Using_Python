#!/usr/bin/env python
# encoding: utf-8
'''
@author: fanyuexiang
@software: pycharm
@file: A_Gentle_Introduction_to_Spark.py
@time: 2020/5/12 10:53 下午
@desc: Spark权威指南第二章 Spark浅析
'''
import os
from pprint import pprint

from pyspark import *
from pyspark.sql import SparkSession

PYSPARK_PYTHON ="/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

# 开发Spark应用程序：第一步创建SparkSession，SparkSession运行在JVM中，用于控制Application
spark = SparkSession.builder.appName("chap02").master("local[*]").getOrCreate()
'''
    自定义创建Spark结构化数据结构——dataFrame，与Pandas有相似的地方
    Spark的DataFrame可以根据分区规则在多台机器上并行计算
    Pandas的DataFrame只能在单台机器本地执行
'''
my_range = spark.range(100).toDF("number")
my_range.show()
'''
    查看DataFrame的分区，注意本地启动Spark与集群上运行Spark分区规则有区别
    由于启动时指定了master节点为local[*]，因此创建Spark并行度为本地CPU核数
'''
print(my_range.rdd.getNumPartitions())

''''
    Spark 转换操作，Spark转换操作涉及shuffle，根据转换算子是否需要执行shuffle
    将Spark转换算子之间的依赖关系划分为：宽依赖与窄依赖，Spark Application的阶段划分
    根据宽依赖划分，因为窄依赖均可以在同一台机器上执行完毕
'''
div_is_y2 = my_range.where("number % 2 == 0")
# Spark所有执行均为惰性执行，只有遇到行动操作才会物理执行；之前是建立一系列转换关系
div_is_y2.show()

# Spark读取CSV文件，设置文件头，Spark执行推理模式，尽可能推断每列字段类型
flight_data_2015 = spark.read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Spark_The_Definitive/data/flight-data/csv/2015-summary.csv")

pprint(flight_data_2015.take(3))
'''
    从下到上查看执行计划，可以找出各个宽窄依赖
    == Physical Plan ==
    *(3) Sort [count#35L ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(count#35L ASC NULLS FIRST, 200)
       +- *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#22], functions=[count(1)])
          +- Exchange hashpartitioning(DEST_COUNTRY_NAME#22, 200)
             +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#22], functions=[partial_count(1)])
                +- *(1) FileScan csv [DEST_COUNTRY_NAME#22] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Spark_The..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>
                
    第一步：读取CSV文件；
    第二步：HashAggregate 根据DEST_COUNTRY_NAME列的Hash值聚合
    第三步：hashpartitioning 根据DEST_COUNTRY_NAME列的Hash值 重新分区
    第四步：     
'''
dataFrameWay = flight_data_2015.groupBy("DEST_COUNTRY_NAME").count().sort("count")
dataFrameWay.explain()

