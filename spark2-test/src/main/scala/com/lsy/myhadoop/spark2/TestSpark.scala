package com.lsy.myhadoop.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {

  def main(args: Array[String]): Unit = {
    println("teh dd")


    val logFile = "/user/hdfsfile1"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    println(logData.first())

    val numAs = logData.filter(line => line.contains("li")).count()
    val numBs = logData.filter(line => line.contains("tom")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
  }

//  def main(args: Array[String]): Unit = {
//    println("teh dd")
//    map()
//    println("teh dd")
//  }

  def map() {
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)
    val numberRDD = sc.parallelize(numbers, 1)
    val multipleNumberRDD = numberRDD.map { num => num * 2 }

    multipleNumberRDD.foreach { num => println(num) }
  }

}
