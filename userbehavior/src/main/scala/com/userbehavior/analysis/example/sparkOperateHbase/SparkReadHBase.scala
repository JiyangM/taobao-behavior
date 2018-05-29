package com.userbehavior.analysis.example.sparkOperateHbase

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark 读取hbase数据并转化为dataFrame
  */
object SparkReadHBase {

  def main(args: Array[String]): Unit = {

    // 本地模式便于测试
    val sparkConf = new SparkConf().setMaster("local").setAppName("dataToHbase")

    // 创建hbase configuration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"student")
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"192.168.242.100:2181")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val stuRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count();
//    stuRDD.cache()

//    stuRDD.foreach({ case (_,result)=>{
//        val key = Bytes.toString(result.getRow)
//        val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
//        val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
//        val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
//        println("Row key:"+key+" Name:"+name+" Gender:"+gender+" Age:"+age)
//      }
//    })

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val stuDF = stuRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("age")))
    )).toDF("name","age")

    stuDF.registerTempTable("student")
    val df2 = sqlContext.sql("select * from student")
//    df2.foreach(println)
  }
}
