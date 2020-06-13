package com.lsy.myhadoop.spark2.test1.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by lisiyu on 2020/5/24.
  */
object DfConvert {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length == 0) {
      conf.setMaster("local[1]")
    }

    val spark = SparkSession
      .builder()
      .appName("FantasyBasketBall")
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val abc = Array("Runoob", "Baidu", "Google")
    val fs = "1,2,3,4".split(",").map(f => StructField(f, StringType))


    val schema = StructType(fs)
//    val auctions = spark.createDataFrame(data, schema)
//    auctions.printSchema
//    auctions.show()
//    println(auctions.first())
  }


}
