#!/bin/bash
export spark_home=/opt/spark/spark-2.4.4-bin-hadoop2.7
$SPARK_HOME/bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \ # 运行主类
    --master yarn local,standlone \ #Spark部署模式
    --deploy-mode client,cluster \ #客户端模式或集群提交模式
    --executor-memory 2G\
    --num-executors 10 \ # executor数量
    xxx.jar  # 本地Jar包位置
    parameters


