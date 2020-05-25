#!/usr/bin/env python
# encoding: utf-8
"""
@author: fanyuexiang
@software: pycharm
@file: Different_Types_of_Data.py
@time: 2020/5/22 5:45 上午
@desc: Spark权威指南第6章 处理不同类型的数据
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, instr, expr, lit, bround, round, corr, monotonically_increasing_id, initcap, \
    lower, upper, ltrim, rtrim, trim, lpad, rpad, regexp_replace, translate, regexp_extract, locate, current_timestamp, \
    current_date, date_sub, date_add, datediff, to_date, months_between, to_timestamp, coalesce, struct, split, explode, \
    to_json

PYSPARK_PYTHON = "/Library/Frameworks/Python.framework/Versions/3.6/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark = SparkSession.builder.appName("chap06") \
    .master("local[*]").getOrCreate()
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(
    "/Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Spark_The_Definitive_Guide/data/retail-data/by-day/2010-12-01.csv")
# 处理布尔类型
# 指定大于，小于，等于
df.where(col("InvoiceNo") != 536365).select("InvoiceNo", "Description") \
    .show(5, False)

df.where("InvoiceNo = 536365").show(5, False)
# 使用链式连接的方式，执行顺序过滤器
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show(5, False)

DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)) \
    .where("isExpensive").select("unitPrice", "isExpensive").show(5, False)
# 处理数值类型，在Spark中，只需要简单地表达计算方法，并且确保计算表达式对数值类型数据正确可行即可
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(5, False)
# 使用SQL表达式实现
df.selectExpr(
    "CustomerId",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
# round向上取整，bound向下取整
df.select(bround(lit(2.5)), round(lit(2.5))).show(2)
# 计算两列的相关性
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()
# 统计列/一组列的相关性
df.describe().show()
# statFunction封装了很多统计函数
colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError)
# 查看两列的交叉列表
df.stat.crosstab("StockCode", "Quantity").show()
# 查看频繁项
df.stat.freqItems(["StockCode", "Quantity"]).show(2, False)
# 为每一行生成唯一ID
df.select(monotonically_increasing_id().alias("id")).show(2)
# 处理字符串类型
# initcap将空格分隔的字符串的单词首字母大写
df.select(initcap(col("Description"))).show()
# 字符串大小写转换
df.select(col("Description"), lower(col("Description")),
          upper(lower(col("Description")))).show(2)
# 字符串删除空格或者在其周围添加空格，lpad或rpad根据输入参数值与输入字符串长度比较，决定删除字符串长度
df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("    HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)
# Spark使用这则表达式过滤字符串
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
    regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
    col("Description")).show(2, False)
# translate替换字符串，对比入参逐个字符替换，例如下面例子，L-1，E-3， T-7
df.select(translate(col("Description"), "LEET", "1337"), col("Description")) \
    .show(2, False)
# regexp_extract用于提取执行出现顺序的字符串，下面例子中extract_str任意单词出现在第1个位置则被提取
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
    regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
    col("Description")).show(2)

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite) \
    .where("hasSimpleColor") \
    .select("Description").show(3, False)

simpleColors = ["black", "white", "red", "green", "blue"]


def color_locator(column, color_string):
    return locate(color_string.upper(), column) \
        .cast("boolean") \
        .alias("is_" + color_string)


selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*"))  # has to a be Column type
df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)
# 处理日期和时间类型
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()
dateDF.show(5, False)
# date_add增加/date_sub减去天数
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
# datediff计算日期间的时间间隔
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)
# months_between计算日期间隔的月数；to_date以指定格式将字符串转换为日期数据
dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end")).cast("int")).show(1)

spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)
# 以指定日期格式转换日期
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")
# to_date和to_timestamp区别：前者可选择一种日期格式，后者强制要求使用一种日期格式
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
# 确保使用同一种日期格式后，可以对日期进行过滤和比较操作
# cleanDateDF.filter(col("data2") > lit("2017-12-12")).show()
# 处理数据中的空值：显式的删除空值或使用特定值填充空值

# coalesce函数实现从一组列中选择第一个非空值列
df.select(coalesce(col("Description"), col("CustomerId"))).show(truncate=False)
# drop删除包含null的行，subset指定对某些列的行进行操作
df.na.drop("all", subset=["StockCode", "InvoiceNo"])
# COMMAND ----------
df.na.fill("all", subset=["StockCode", "InvoiceNo"])
# fill函数通过一组值(映射)填充多列
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)

# 处理复杂结构类型
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")
complexDF.show()
# 使用split指定分隔符
df.select(split(col("Description"), " ")).show(2, truncate=False)
# 返回每一行中数组的第一个元素
df.select(split(col("Description"), " ").alias("array_col"))\
  .selectExpr("array_col[0]").show(2)
# explode函数的输入参数为一个包含数组的列，并未该数组中的每个值创建一行(每行复制该值)
df.withColumn("splitted", split(col("Description"), " "))\
  .withColumn("exploded", explode(col("splitted")))\
  .select("Description", "InvoiceNo", "exploded").show(10, truncate=False)

# map构造两列内容映射的键值对形式
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")))

# 用户自定义函数(UDF)
'''
    1. 可以使用Python，Java，Scala编写UDF函数，但是推荐使用Java和Scala编写
    2. Spark上将注册UDF以便在所有工作机器上使用，Spark在驱动器进程上序列化该函数，并将
    它们通过网络传输到所有执行进程
    3. 如果使用Java或Scala编写，则可在Java虚拟机中使用；如果使用Python编写，Spark在Worker
    上启动一个Python进程，将所有数据转换为Python可解释的格式，在Python进程中针对该数据逐行执行
    函数，结果返回给JVM和Spark。
'''
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
parseSchema = StructType((
  StructField("InvoiceNo",StringType(),True),
  StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")).alias("newJSON"))\
  .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)


# COMMAND ----------

udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value ** 3
power3(2.0)


# COMMAND ----------
# 向Spark注册UDF函数
from pyspark.sql.functions import udf
power3udf = udf(power3)


# COMMAND ----------

from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col("num"))).show(2)


# COMMAND ----------

udfExampleDF.selectExpr("power3(num)").show(2)
# registered in Scala


# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType
spark.udf.register("power3py", power3, DoubleType())


# COMMAND ----------

udfExampleDF.selectExpr("power3py(num)").show(2)
