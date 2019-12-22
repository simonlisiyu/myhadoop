package com.lsy.myhadoop.spark2.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionUtils {
  private var spark:SparkSession = null

  def getOrInitSparkSession(appName:String): SparkSession = {
    if(spark == null){
      spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    }
    spark
  }

  def getOrInitSparkSession(conf:SparkConf, appName:String): SparkSession = {
    if(spark == null){
      spark = SparkSession.builder().config(conf).appName(appName).enableHiveSupport().getOrCreate()
    }
    spark
  }

  def readTextFile(spark:SparkSession, path:String) = {
    spark.read.textFile(path)
  }

}
