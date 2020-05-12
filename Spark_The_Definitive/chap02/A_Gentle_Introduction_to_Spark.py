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
from pyspark.sql.functions import desc

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
    第二步：HashAggregate 根据DEST_COUNTRY_NAME列的Hash值聚合，会改变分区，2-4步涉及shuffle
    第三步：hashpartitioning 根据DEST_COUNTRY_NAME列的Hash值 重新分区，默认情况输出200个shuffle分区
    第四步：HashAggregate count计数，与group都会有shuffle操作
    第五步：rangepartitioning sort排序，不改变分区     
'''
dataFrameWay = flight_data_2015.groupBy("DEST_COUNTRY_NAME").count().sort("count")
dataFrameWay.explain()

# 改变spark宽依赖的shuffle分区数量
spark.conf.set("spark.sql.shuffle.partitions", "5")
dataFrameWay_Opm = flight_data_2015.groupBy("DEST_COUNTRY_NAME").count().sort("count")
dataFrameWay_Opm.explain()

'''
    上面介绍了Spark DataFrame，下面对Spark SQL进行介绍，涉及以下几个转换操作：
    1. 注册临时表
'''
flight_data_2015.createOrReplaceTempView("flight_data_table_2015")
sqlWay = spark.sql("""
Select DEST_COUNTRY_NAME, count(1) as _count
From flight_data_table_2015
group by DEST_COUNTRY_NAME
""")
sqlWay.show()
# Spark SQL与DataFrame的物理执行计划相同
sqlWay.explain()

maxCount = flight_data_2015.groupBy("DEST_COUNTRY_NAME")\
    .sum("count").withColumnRenamed("sum(count)", "destination_total")\
    .sort(desc("destination_total")).limit(5)

maxSql = spark.sql("""
Select DEST_COUNTRY_NAME, sum(count) as destination_total
From flight_data_table_2015
group by DEST_COUNTRY_NAME 
order by destination_total desc
limit 5
""")
'''
    上面Spark转换操作是一个有向无环图(DAG)，每个转换产生一个新的不可变的DataFrame
    完整转换流程如下：
        第一步：读取数据
        第二步：groupBy 按照指定列分组
        第三步：sum聚合操作
        第四步：withColumnRenamed 重命名列
        第五步：orderBy 排序
        第六步：limit
        第七步：行动操作
'''
maxSql.show()
maxCount.show()
maxSql.explain()



