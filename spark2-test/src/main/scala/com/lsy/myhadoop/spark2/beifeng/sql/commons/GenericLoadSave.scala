package com.lsy.myhadoop.spark2.beifeng.sql.commons

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
 * @author Administrator
 */
object GenericLoadSave {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("GenericLoadSave")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    val usersDF = sqlContext.read.load("hdfs://spark1:9000/users.parquet")
    usersDF.write.save("hdfs://spark1:9000/namesAndFavColors_scala")  
  }
  
}