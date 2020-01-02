package com.lsy.myhadoop.spark2.beifeng.core.count

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lisiyu on 2016/3/28.
 */
object NumOnce {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("numonce").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(Array(1,2,2,1,3,4,3))
    val result = lines.mapPartitions(x => {
      var temp = x.next()
      while(x.hasNext){
        temp = temp^x.next()
      }
      Iterator(1 -> temp)
    }).reduceByKey(_^_).collect()

    println(result(0))
  }

}
