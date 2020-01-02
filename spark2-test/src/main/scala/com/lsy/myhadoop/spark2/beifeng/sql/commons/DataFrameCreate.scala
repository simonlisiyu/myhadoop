package com.lsy.myhadoop.spark2.beifeng.sql.commons

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * @author Administrator
 */
object DataFrameCreate {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("DataFrameCreate")
        .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    val df = sqlContext.read.json("hdfs://192.168.190.132:9000/test/students.json")
    
    df.show()  
  }
  
}