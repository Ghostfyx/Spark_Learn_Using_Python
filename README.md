# Spark_Learn_Using_Python
使用Python实现Spark书中各个章节的代码
## 1. Spark快速大数据分析

## 2. Spark权威指南
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

