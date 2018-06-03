# 简介
本项目通过淘宝用户数据集进行统计分析

## 使用技术
Hadoop、Hive、Spark、Hbase、python matplotlib（数据展示）

## 数据来源
https://tianchi.aliyun.com/datalab/dataSet.html?spm=5176.100073.0.0.6f3635ee884jXd&dataId=649

本数据集包含了2017年11月25日至2017年12月3日之间，有行为的约一百万随机用户的所有行为（行为包括点击、购买、加购、喜欢）。数据集的组织形式和MovieLens-20M类似，即数据集的每一行表示一条用户行为，由用户ID、商品ID、商品类目ID、行为类型和时间戳组成，并以逗号分隔

## 数据量
原始csv文件2.05G

所有行为数量 100,150,807

## 操作流程
- 数据集下载

- 创建hive表
```
create table user_behaviors(userId int,itemId int,categoryId int,behaviorType string,times string)
row format delimited
fields terminated by ','
stored as textfile;
```
- 数据集文件导入hive表
```
load data local inpath '/usr/local/apps/hive/ff/' into table user_behaviors;
```

根据hdfs将文件分块存储策略 2*1024M/128M 约等于16，所以共有16个 block；

- spark job分析任务
  
  1.在应用层面分析，宏观上分析应用客户的使用情况， 统计每日、每个事件行为（行为包括点击、购买、加购、喜欢）触发的人数和次数。
 
  2.在商品层面分析，查看某一类 统计每日、每类商品事件的触发次数结果存入hbase

  - 读取csv文件，将数据隐式转成Dataframe,将数据存入Hbase
  
  
  
  ![image](https://github.com/JiyangM/Taobao-user-behavior/blob/master/image/euev.png)
  
  
- 数据展示：

