package com.userbehavior.analysis.example.sparkOperateHbase

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteHbase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHbase").setMaster("local")
    val sc = new SparkContext(sparkConf)

    // 创建hbase configuration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"student")
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"192.168.242.100:2181")

    val tablename ="student"

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    // 构建两行记录
    val indataRDD = sc.makeRDD(Array("3,zhangsan,M,26","4,lisi,M,27"))
    val rdd = indataRDD.map(_.split(",")).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3)))
      (new ImmutableBytesWritable(),put)
    }}
    rdd.saveAsNewAPIHadoopDataset(jobConf)
    sc.stop()
  }
}
