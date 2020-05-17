package com.lsy.myhadoop.spark2.esspark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
import scala.collection.{Map, mutable}

/**
  * Created by lisiyu on 2020/4/26.
  */
class Es2rdd2Es {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("appName")
      .config("spark.sql.warehouse.dir", "hdfs://master:9000/usr/hive/warehouse")
      .config("es.nodes", "192.168.123.111:9200,192.168.123.112:9200") //设置es.nodes
      .config("pushdown", "true") //执行sql语句时在elasticsearch中执行只返回需要的数据。这个参数在查询时设置比较有用
      .config("es.index.auto.create", "true") //如果没有这个index自动创建
      .config("es.nodes.wan.only", "true")
      .enableHiveSupport()
      .getOrCreate()
    val query =
      """
        {"query":{"bool":{"must":[{"match_phrase":{"hobbies":"网球"}}]}}}
      """
    import org.elasticsearch.spark._
    val esrdd: RDD[(String, Map[String, AnyRef])] = spark.sparkContext.esRDD("index/type", query)
    val map: RDD[mutable.Map[String, AnyRef]] = esrdd.map(r => {
      val key = r._1
      val value = r._2
      import scala.collection.mutable.Map
      //我这里Map泛型为[String,AnyRef]因为我的数据中有object类型，这里需要指定泛型。
      val tag: mutable.Map[String, AnyRef] = Map("profession" -> "IT")
      for (tmp <- value) {
        tag += (tmp._1 -> tmp._2)
      }
      tag
    })
    try {
      EsSpark.saveToEs(map, "index1/type1")
    } catch {
      case ex: Exception => {
        print(ex)
      }
    }
  }
}
