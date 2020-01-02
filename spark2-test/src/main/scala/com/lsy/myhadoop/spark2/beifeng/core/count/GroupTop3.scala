package com.lsy.myhadoop.spark2.beifeng.core.count

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 3/27/16.
  */
object GroupTop3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("top3group").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/root/Desktop/score")
    val pairs = lines.map(x => {
      (x.split(" ")(0), x.split(" ")(1).toInt)
    })
    val group = pairs.groupByKey();
    val top3group = group.map(g => {
      var v1 = 0
      var v2 = 0
      var v3 = 0
      for(v <- g._2){
        if(v > v1){
          v3 = v2
          v2 = v1
          v1 = v
        }else if(v > v2){
          v3 = v2
          v2 = v
        }else if(v > v3){
          v3 = v
        }
      }
      (g._1, (v1,v2,v3))
    })
    top3group.foreach(println)
  }

}
