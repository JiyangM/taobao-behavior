package com.userbehavior.analysis.etl

import com.userbehavior.analysis.example.rddToDataFrame.CreateDataFrame

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
    val sql = "select behaviorType,count(1) as ev,count(distinct(userId)) as eu,dayId from behaviors GROUP BY behaviorType,dayId"
    val dataDF = CreateDataFrame.frame(filePath, sql)
  }


  /**
    * 商品层面统计
    * 统计每日、每类商品事件的触发次数
    */
  def trigger(filePath: String): Unit = {
    val sql = "select categroyId,count(1) as triggerNum,dayId from behaviors group by dayId,categroyId,behaviorType"
    val dataDF = CreateDataFrame.frame(filePath, sql)
  }


  def main(args: Array[String]): Unit = {
    trigger("D:\\py\\UserBehavior.csv")
  }

}
