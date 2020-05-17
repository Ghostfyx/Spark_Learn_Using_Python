#!/usr/bin/env python
# encoding: utf-8
"""
@author: fanyuexiang
@software: pycharm
@file: Basic_Structured_Operations.py
@time: 2020/5/18 4:18 上午
@desc: Spark权威指南第五章 基本结构化操作
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, column, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, Row

PYSPARK_PYTHON = "/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark = SparkSession.builder.master("local[*]").appName("chap05").getOrCreate()
# 模式可以由数据源来定义模式，称为读时模式(schema-on-read)，也可以由我们自己显式定义
flight_data = spark.read.format("json").load("/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Spark_The_Definitive_Guide/data/flight-data/json/2015-summary.json")
"""
    模式由许多字段构成的StructType，字段即StructField，具有名称、类型、布尔标志(该字段是否可为空)
    并且用户可指定与该列相关联的元数据，元数据存储着有关此列的信息(Spark在机器学习库中使用此功能)
"""
flight_data.printSchema()

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", LongType(), False, metadata={"hello": "world"})
])
flight_data_schema = spark.read.format("json").schema(myManualSchema).load("/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Spark_The_Definitive_Guide/data/flight-data/json/2015-summary.json")
flight_data_schema.show()
# 列和表达式
"""
    对于Spark而言，列是逻辑结构，它只是表示根据表达式为每个记录计算出的值，这意味着要为一列
    创建出一个真值，有一行则需要有一个DataFrame，不能在DataFrame外操作一列
"""
col("someColumnName")
# 如果需要引用DataFrame中的某一列，则可在DataFrame上使用col方法，
# 显式引用的一个好处是Spark不用自己解析该列
count = flight_data_schema["count"]
print(count)
# 表达式是DataFrame中某一个记录的一个或多个值的一组转换操作，将一个或多个列名作为输入解析，
# 然后针对数据集中的每条记录应用表达式来得到第一个值
expr("(((someCol + 5)*200) - 6) < otherCol")
columns = flight_data_schema.columns
print(columns)
# 记录和行，DataFrame中的每一行都是一个记录
myRow = Row("Hello", None, 1, False)
# 访问行中的数据只需要指定想要的位置
print(myRow[0])
print(myRow[1])
# 创建DataFrame
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()
# DataFrame中的转换操作
flight_data_schema.createOrReplaceTempView("dfTable")
# 使用不同的方式对列引用
flight_data_schema.select("DEST_COUNTRY_NAME").show(2)
flight_data_schema.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
flight_data_schema.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)
# 使用expr表达式
flight_data_schema.select(expr("DEST_COUNTRY_NAME as destination")).show(2)
flight_data_schema.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)
# selectExpr
flight_data_schema.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

flight_data_schema.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)
# selectExpr使用系统预定义好的聚合函数来指定整个DataFrame上的聚合操作
flight_data_schema.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
# 使用lit函数传递给Spark字面常量
flight_data_schema.select(expr("*"), lit(1).alias("One")).show(2)
# 使用withColumn添加列, withColumn有两个参数：列名和给定行赋值的列表达式
flight_data_schema.withColumn("numberOne", lit(1)).show(2)
flight_data_schema.withColumn("withinCountry", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")).show(2)
# 使用withColumnRenamed重命名列
print(flight_data_schema.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns)
