package com.userbehavior.analysis.example.sparkOperateHbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.spark.{SparkConf, SparkContext}


object SparkWriteHbase2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark-write-hbase").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    val tableName = "student"
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()

    val hTableDesc = new HTableDescriptor(tableName)
    hTableDesc.addFamily(new HColumnDescriptor("name"))
    hTableDesc.addFamily(new HColumnDescriptor("contactinfo"))
    hTableDesc.addFamily(new HColumnDescriptor("address"))

    admin.createTable(hTableDesc)
  }

}
