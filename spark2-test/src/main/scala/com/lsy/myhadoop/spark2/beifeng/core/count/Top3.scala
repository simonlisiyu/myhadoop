package com.lsy.myhadoop.spark2.beifeng.core.count

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Administrator
 */
object Top3 {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("Top3")
        .setMaster("local")  
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("/home/softwares/spark-env-dir/hadoop-2.6.0-cdh5.4.2/top.txt", 1)
    val pairs = lines.map { line => (line.toInt, line) }
    val sortedPairs = pairs.sortByKey(false)
    val sortedNumbers = sortedPairs.map(sortedPair => sortedPair._1)  
    val top3Number = sortedNumbers.take(3)
    
    for(num <- top3Number) {
      println(num)  
    }
  }
  
}