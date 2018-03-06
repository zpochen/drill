# General概述 

Apacche Drill 是一个分布式的MPP计算框架，支持使用SQL查询多种存储框架（MongoDB、MySQL、Hadoop等）。本项目主要升级了Apache Drill对kudu的查询支持。

Apache Drill is a distributed MPP query layer that supports SQL and alternative query languages against NoSQL and Hadoop data storage systems.  This project mainly enhance query support of kudu. 

## Install使用安装

参考:http://drill.apache.org/

## Updates功能更新

1. fix the bugs of kudu client which version is 1.3.0.更新了对kudu新版本（1.3.0版）的支持，修复了多个bug.

2. push down the query filter of sql into kudu predicate,so kudu query performance will improve greatly. 将查询过滤条件下推到kudu predicate,会大大提高kudu 的查询性能

3. support parition to filter kudu data. 支持分区过滤

4. fix the bugs when using hbase join with other storage systems.




