package com.lsy.myhadoop.spark2.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object CommonUtil {

  // 解析string为日期类型
  def parseDate(dateString: String): Option[LocalDate] = {
    try {
      val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val date = LocalDate.parse(dateString, dateTimeFormatter)
      Some(date)
    } catch {
      case _: Throwable => {
        None
      }
    }
  }

  // 根据传入的参数获取当前日期以前日期列表
  def getBeforeDates(dateString: String, numberDates: Int): IndexedSeq[String] = {
    parseDate(dateString) match {
      case Some(date: LocalDate) =>
        (0 until  numberDates).map(i =>
          date.minusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        )

      case _ => IndexedSeq[String]()
    }
  }

  def checkOutput(path: String) : Unit = {
    if (!path.contains("/user/its_bi/lisiyu/")) {
      System.err.println("output error")
      System.exit(1)
    }
  }

  def checkOutputSave(path: String, savePath: String) : Unit = {
    if (!path.contains(savePath)) {
      System.err.println("output error")
      System.exit(1)
    }
  }
}
