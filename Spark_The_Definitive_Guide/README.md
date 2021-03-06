# 《Spark权威指南》

每章代码与概述

## 2. Spark权威指南每章概述
### 2.1 第一章 Spark是什么

Spark设计哲学需要有三点：

- 统一平台
- 计算引擎
- 配套软件库 

Spark可以理解为MapReduce的升级版，为了计算MapReduce计算框架的如下问题：

1. 与HDFS耦合严重，生产环境中使用MapReduce数据来源单一，必须依赖于HDFS；
2. MapReduce计算缓慢，因为MapReduce中，一个Application对应一个Job，一个Job对应一个Map阶段和一个Reduce阶段，如果需要迭代计算则不断的开启进程，杀死进程，中间结果存储
在HDFS上给后续Application提供输入源，浪费资源；
3. Mapper的map函数与Reducer的reduce对应数据加工/清洗过程，对于可通用的逻辑，每次用户需要手动实现；
4. MapReduce的shuffle过程

还有其他缺点，后期进行补充

### 2.2 第二章 Spark浅析

### 2.3 第三章 Spark工具集介绍
涉及以下内容：
1. 使用Spark-submit运行应用程序
2. Dataset:类型安全的结构化数据结构API
3. 结构化流处理
4. 机器学习与高级分析
5. 弹性分布式数据集RDD，Spark低级API
6. SparkR
7. 第三方软件包生态系统

### 2.4 第四章 Spark结构化API概述
结构化API是处理各种数据类型的工具，可以是非结构化的日志文件、半结构化的CSV文件，以及高度结构化的Parquet文件，结构化API
以下三种核心分布式集合类型的API：
- Dataset类型
- DataFrame类型：本质上是Dataset<Row> 
- SQL表和视图

Spark内部使用一个名为Catalyst的引擎，在计划制定和执行作业的过程中使用Catalyst来维护自己的类型信息，
这样就会带来很大的优化空间。

Spark结构化API的查询任务大致执行步骤如下：

- 编写DataFrame/Dataset/SQL代码；
- Spark将其转换为一个逻辑执行计划(Logical Plan)；
- Spark将此逻辑执行计划转换为一个物理执行计划(Physical Plan)，检查可行的优化策略，并在此过程中检查优化；
- Spark在集群上执行该物理执行话(RDD操作)

PS：物理执行计划的优化通过代价模型进行分析比较，经过分析数据表的物理属性(表的大小或分区的大小)，对不同的物理执行
策略进行比较，选择合适的物理执行计划。

### 第五章 基本的结构化操作

DataFrame由记录(record)组成，record是Row类型(与一个table中的行相似)。一条record由多列组成，列
表示可以在该DataFrame中每个单独的记录上执行的计算表达式。模式定义了DataFrame列的名以及列的数据类型。
DataFrame分区定义了DataFrame以及Dataset在集群上的物理分布，而划分模式定义了partition的分配模式，
可以自定义分区模式，也可以采取随机分配的方式。

以下为本章重点介绍的几部分：
- 模式
- 列和表达式
- 记录和行
- DataFrame转换操作

DataFrame的转换操作被归结为以下几类：
- 添加行或列
- 删除行或列
- 将一行转换操作为一列(或将一列转换操作为一行)
- 根据列中的值更改行的顺序

### 第六章 处理不同类型的数据

第五章介绍关于DataFrame的基本概念和抽象，本章将介绍表达式的构建
这是Spark结构化操作的基础，还将介绍对不同数据类型的处理，主要包括以下内容：

- 布尔型
- 数字型
- 字符型
- 日期和时间类型
- 空值处理
- 复杂类型
- 用户自定义的函数



