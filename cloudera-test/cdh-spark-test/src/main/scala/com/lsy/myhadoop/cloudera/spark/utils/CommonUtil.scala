package com.lsy.myhadoop.cloudera.spark.utils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}
import java.util.Calendar

object CommonUtil {

  // 解析string为日期类型
  def parseDate(dateString: String, pattern: String = "yyyyMMdd"): Option[LocalDate] = {
    try {
      val dateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
      val date = LocalDate.parse(dateString, dateTimeFormatter)
      Some(date)
    } catch {
      case _: Throwable => {
        None
      }
    }
  }

  def parseDateInfo(dateString: String, pattern: String = "yyyyMMdd"): (Int, Int, Int) = {
    try {
      val dateTimeFormatter = new SimpleDateFormat(pattern)
      val date = dateTimeFormatter.parse(dateString)
      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
    } catch {
      case _: Throwable => (-1, -1, -1)
    }
  }

  // 获取上周末
  def lastWeekDate(date: LocalDate): LocalDate = {
    date.getDayOfWeek() match {
      case DayOfWeek.SUNDAY => date.minusDays(7)
      case dayOfWeek => date.minusDays(dayOfWeek.ordinal() + 1)
    }
  }

  /**
    * 当前这个日期往前计算几天, 分周末和工作日
    *
    * @param date
    * @return IndexedSeq[String]
    */
  def getBeforeDatesByDateType(date: LocalDate, num: Int, pattern: String = "yyyyMMdd"): IndexedSeq[String] = {
    isWeekend(date) match {
      case true => (0 until num).map(i => date.minusDays(i)).filter(isWeekend).map(formatUtcDate)
      case false => (0 until num).map(i => date.minusDays(i)).filter(item => !isWeekend(item)).map(formatUtcDate)
    }
  }

  def main(args: Array[String]): Unit = {
    val dates = CommonUtil.getBeforeDatesByDateType(LocalDate.of(2017, 12, 13), 28)

    val sqlDates = dates.mkString("'", "','", "'")
    println(sqlDates)
  }

  // 判断是否是周末
  def isWeekend(date: LocalDate): Boolean = {

    date.getDayOfWeek() match {
      case DayOfWeek.SATURDAY | DayOfWeek.SUNDAY => true
      case _ => false
    }
  }

  // 格式化utc日期
  def formatUtcDate(date: LocalDate): String = {
    date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }

  // 根据传入的参数获取当前日期以前日期列表
  def getBeforeDates(dateString: String, numberDates: Int, pattern: String = "yyyyMMdd"): IndexedSeq[String] = {
    parseDate(dateString, pattern) match {
      case Some(date: LocalDate) =>
        (0 until numberDates).map(i =>
          date.minusDays(i).format(DateTimeFormatter.ofPattern(pattern))
        )

      case _ => IndexedSeq[String]()
    }
  }

  def checkOutput(path: String): Unit = {
    if (!path.contains("/user/lisiyu/")) {
      System.err.println("output error")
      System.exit(1)
    }
  }

  def lift2[A, B, C](f: Function2[A, B, C]): Function2[Option[A], Option[B], Option[C]] =
    (oa: Option[A], ob: Option[B]) => for (a <- oa; b <- ob) yield f(a, b)

}
