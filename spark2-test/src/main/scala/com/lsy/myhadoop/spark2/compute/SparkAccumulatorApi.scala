package com.lsy.myhadoop.spark2.compute

import com.lsy.scala.util.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{AccumulatorV2, random}
import org.slf4j.LoggerFactory

object SparkAccumulatorApi {

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    // accumulator 通常用于监控，调试，记录符合某类特征的数据数目等
    val total_counter = sc.accumulator(0L, "total_counter")
    val counter0 = sc.accumulator(0L, "counter0")
    val counter1 = sc.accumulator(0L, "counter1")

    val slices = 200
    val n = slices*1000
    val count = sc.parallelize(1 to n, slices).map(x => {
      total_counter += 1
      val num = if (x > 5000) {counter1 += 1; 1} else {counter0 += 1; 0}
      num
    }).reduce(_ + _)

    // AccumulatorV2
    val accumulator = new MyAccumulatorV2()
    sc.register(accumulator)
    accumulator.add("123")
    println(accumulator.value)
  }

}


class MyAccumulatorV2 extends AccumulatorV2[String, String] {

  private val log = LoggerFactory.getLogger("MyAccumulatorV2")

  var result = "user0=0|user1=0|user2=0|user3=0" // 初始值

  override def isZero: Boolean = {
    true
  }

  override def copy(): AccumulatorV2[String, String] = {
    val myAccumulator = new MyAccumulatorV2()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = "user0=0|user1=0|user2=0|user3=0"
  }

  override def add(v: String): Unit = {
    val v1 = result
    val v2 = v
    //    log.warn("v1 : " + v1 + " v2 : " + v2)
    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
      var newResult = ""
      // 从v1中，提取v2对应的值，并累加
      val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue != null) {
        val newValue = oldValue.toInt + 1
        newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
      }
      result = newResult
    }
  }

  override def merge(other: AccumulatorV2[String, String]) = other match {
    case map: MyAccumulatorV2 =>
      result = other.value
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: String = {
    result
  }

}

/**
  *类继承AccumulatorV2
  * class MyAccumulatorV2 extends AccumulatorV2[String, String]
  * 覆写抽象方法：
  */
class MyAccumulatorV2_Test extends AccumulatorV2[String, String] {

  // isZero: 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
  override def isZero: Boolean = ???

  // copy: 拷贝一个新的AccumulatorV2
  override def copy(): AccumulatorV2[String, String] = ???

  // reset: 重置AccumulatorV2中的数据
  override def reset(): Unit = ???

  // add: 操作数据累加方法实现
  override def add(v: String): Unit = ???

  // merge: 合并数据
  override def merge(other: AccumulatorV2[String, String]): Unit = ???

  // value: AccumulatorV2对外访问的数据结果
  override def value: String = ???

}