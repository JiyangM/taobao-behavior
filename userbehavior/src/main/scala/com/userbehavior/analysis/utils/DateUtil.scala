package com.userbehavior.analysis.utils

import java.text.SimpleDateFormat
import java.util.Date

/** *
  *
  * @Des: 日期处理
  * @Author: jiyang
  * @Date: 2018-05-30 1:17
  */
object DateUtil {

  /**
    * 时间戳转日期
    */
  def DateFormat(time: String): Long = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var date: String = sdf.format(new Date((time.toLong * 1000l)))
    date.toLong
  }


  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date: String = dateFormat.format(now)
    date
  }


  def main(args: Array[String]): Unit = {
    println(DateFormat("1511544070"))
  }
}
