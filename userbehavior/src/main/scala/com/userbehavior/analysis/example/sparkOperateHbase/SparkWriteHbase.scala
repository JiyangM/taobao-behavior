package com.userbehavior.analysis.example.sparkOperateHbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteHbase {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\py\\Taobao-user-behavior\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")
    System.setProperty("spark.sql.warehouse.dir", "D:\\py\\sparksqlwarehouse")

    val sparkConf = new SparkConf().setAppName("SparkWriteHbase").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "student"

    // 创建hbase configuration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
    hbaseConf.set("hbase.zookeeper.quorum", "mini1,mini2,mini3")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val indataRDD = sc.makeRDD(Array("4,jack,15","2,Lily,16","5,mike,16"))
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()

    // 通过hbase shell命令验证是否创建成功 ： get '表名称','行键'
  }
}
