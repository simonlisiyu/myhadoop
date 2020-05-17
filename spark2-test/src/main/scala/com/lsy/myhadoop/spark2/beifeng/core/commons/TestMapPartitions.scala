package com.lsy.myhadoop.spark2.beifeng.core.commons

import org.apache.spark.sql.SparkSession

/**
  * Created by lisiyu on 2020/4/27.
  * 与map方法类似，map是对rdd中的每一个元素进行操作，
  * 而mapPartitions(foreachPartition)则是对rdd中的每个分区的迭代器进行操作。
  * 如果在map过程中需要频繁创建额外的对象(例如将rdd中的数据通过jdbc写入数据库,
  * map需要为每个元素创建一个链接而mapPartition为每个partition创建一个链接),
  * 则mapPartitions效率比map高的多。

SparkSql或DataFrame默认会对程序进行mapPartition的优化。
  */
object TestMapPartitions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("appName")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    val a = sc.parallelize(1 to 9, 3)
    def mapDoubleFunc(a : Int) : (Int,Int) = {
      (a,a*2)
    }
    val mapResult = a.map(mapDoubleFunc)

    println(mapResult.collect().mkString)
    //(1,2)(2,4)(3,6)(4,8)(5,10)(6,12)(7,14)(8,16)(9,18)


    val b = sc.parallelize(1 to 9, 3)
    def doubleFunc(iter: Iterator[Int]) : Iterator[(Int,Int)] = {
      var res = List[(Int,Int)]()
      while (iter.hasNext)
      {
        val cur = iter.next;
        res .::= (cur,cur*2)
      }
      res.iterator
    }
    val result = b.mapPartitions(doubleFunc)
    println(result.collect().mkString)
    //(3,6)(2,4)(1,2)(6,12)(5,10)(4,8)(9,18)(8,16)(7,14)
  }
}
