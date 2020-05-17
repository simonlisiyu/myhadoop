package com.lsy.myhadoop.spark2.esspark

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by lisiyu on 2020/4/26.
  */
class Rdd2Es {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("appName")
      .config("spark.sql.warehouse.dir","hdfs://master:9000/usr/hive/warehouse")
      .config("es.nodes","192.168.123.111:9200,192.168.123.112:9200")   //设置es.nodes
      .config("pushdown", "true")   //执行sql语句时在elasticsearch中执行只返回需要的数据。这个参数在查询时设置比较有用
      .config("es.index.auto.create","true")   //如果没有这个index自动创建
      .config("es.nodes.wan.only","true")
      .enableHiveSupport()
      .getOrCreate()
    val a = Map("name" -> "lx","age"->20,"tags"->Array("aaa","bbb"))
    val b = Map("name" -> "wyq","age"->2,"tags"->Array("www","qqq"))
    val d = Map("name" -> "shm","age"->11,"tags"->Array("sss","mmm"))
    val rdd = spark.sparkContext.makeRDD(Seq(a,b,d))
    EsSpark.saveToEs(rdd,"index/type")
  }
}
