package com.userbehavior.analysis.example.rddToDataFrame

import com.userbehavior.analysis.utils.DateUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/** *
  *
  * rdd 转为dataFrome 方式1 ： 通过反射推断
  * 需要定义一个case class，只有case class 才能被Spark隐式的转为Dataframe
  */
object CreateDataFrame {

  case class UserBehavior(userId: Long, itemId: Long, categroyId: Long, behaviorType: String, dayId: Long)

  def frame(filePath: String, sql: String): DataFrame = {
    //spark的底层调用到了hadoop的api，hadoop的api里面用到了hadoop.home.dir环境变量，程序需要指定
    System.setProperty("hadoop.home.dir", "D:\\py\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")
    //从spark2.0开始，spark不再加载‘hive-site.xml'中的设置，也就是说，hive.metastore.warehouse.dir的设置无效。
    //spark.sql.warehouse.dir的默认值为System.getProperty("user.dir")/spark-warehouse，需要在spark的配置文件core-site.xml中设置
    System.setProperty("spark.sql.warehouse.dir", "D:\\py\\sparksqlwarehouse")

    // 定义sparkSession
    val session = SparkSession.builder()
      .appName("refrect-dataframe")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()
    import session.implicits._

    // 数据用case class 封装，将时间戳转为dayId
    val behaviorDF = session.sparkContext.textFile(filePath).map(_.split(","))
      .filter(behavior => behavior.length == 5)
      .map(behavior => {
        UserBehavior(behavior(0).trim.toLong, behavior(1).trim.toLong, behavior(2).trim.toLong, behavior(3).trim,
          DateUtil.DateFormat(behavior(4).trim))
      }).toDF()

    behaviorDF.createOrReplaceTempView("behaviors")

    val dataDF = session.sql(sql)
    dataDF.map(t => "categroyId:" + t(0) + ",triggerNum:" + t(1) + ",dayId:" + t(2)).show(false)
    dataDF
  }


  def main(args: Array[String]): Unit = {
    frame("D:\\py\\UserBehavior.csv","select behaviorType,count(1) as ev,count(distinct(userId)) as eu,dayId from behaviors GROUP BY behaviorType,dayId")
  }

}

