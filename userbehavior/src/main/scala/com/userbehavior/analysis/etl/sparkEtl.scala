package com.userbehavior.analysis.etl

import com.userbehavior.analysis.example.rddToDataFrame.CreateDataFrame
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * spark etl处理
  * 加载统计数据到hbase
  */
object sparkEtl {

  case class UserBehavior(userId: Long, itemId: Long, categroyId: Long, behaviorType: String, dayId: Long)

  /**
    * 应用层面统计
    * 统计每日、每个事件行为（行为包括点击、购买、加购、喜欢）触发的人数和次数
    */
  def euEv(filePath: String): Unit = {

    //spark的底层调用到了hadoop的api，hadoop的api里面用到了hadoop.home.dir环境变量，程序需要指定
    System.setProperty("hadoop.home.dir", "D:\\py\\Taobao-user-behavior\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")
    //从spark2.0开始，spark不再加载‘hive-site.xml'中的设置，也就是说，hive.metastore.warehouse.dir的设置无效。
    //spark.sql.warehouse.dir的默认值为System.getProperty("user.dir")/spark-warehouse，需要在spark的配置文件core-site.xml中设置
    System.setProperty("spark.sql.warehouse.dir", "D:\\py\\sparksqlwarehouse")

    val session = SparkSession.builder()
      .appName("refrect-dataframe")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    val sql = "select behaviorType,count(1) as ev,count(distinct(userId)) as eu,dayId from behaviors GROUP BY behaviorType,dayId"
    val dataDF = CreateDataFrame.frame(filePath, sql, session)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "t_behavior_eveu")
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.72.10:2181")
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "t_behavior_eveu")

    val rdd = dataDF.toJSON.rdd.map {
      arr => {
        val put = new Put(Bytes.toBytes(arr(0) + "_" + arr(3)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviorType"), Bytes.toBytes(arr(0)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ev"), Bytes.toBytes(arr(1)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("eu"), Bytes.toBytes(arr(2)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dayId"), Bytes.toBytes(arr(3)))
        (new ImmutableBytesWritable(), put)
      }
    }
    rdd.saveAsHadoopDataset(jobConf)
    session.stop()
  }


  /**
    * 商品层面统计
    * 统计每日、每类商品事件的触发次数
    */
  def trigger(filePath: String): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\py\\Taobao-user-behavior\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")
    System.setProperty("spark.sql.warehouse.dir", "D:\\py\\sparksqlwarehouse")

    val session = SparkSession.builder()
      .appName("refrect-dataframe")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    val sql = "select categroyId,count(1) as triggerNum,dayId from behaviors group by dayId,categroyId,behaviorType"
    val dataDF = CreateDataFrame.frame(filePath, sql, session)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "t_behavior_eveu")
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.72.10:2181")
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "t_behavior_eveu")

    val rdd = dataDF.toJSON.rdd.map {
      arr => {
        val put = new Put(Bytes.toBytes(arr(0) + "_" + arr(3)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviorType"), Bytes.toBytes(arr(0)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ev"), Bytes.toBytes(arr(1)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("eu"), Bytes.toBytes(arr(2)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dayId"), Bytes.toBytes(arr(3)))
        (new ImmutableBytesWritable(), put)
      }
    }
    rdd.saveAsHadoopDataset(jobConf)
    session.stop()
  }


  def main(args: Array[String]): Unit = {
    euEv("D:\\py\\UserBehavior.csv")
  }

}
