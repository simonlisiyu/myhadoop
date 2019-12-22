package com.lsy.myhadoop.spark2.compute

import org.apache.spark.sql.SparkSession

object SparkPipeApi {

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(List("ab","cd","ef","gh","ij"),2)
    rdd.pipe("/script/test.sh").collect   //Array[String] = Array(Running shell script, Running shell script)
  }
}
