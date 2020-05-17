package com.lsy.myhadoop.spark2.compute

import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory

/**
  * Created by lisiyu on 2020/4/27.
  */
class Df2Map {
  val logger = LoggerFactory.getLogger(this.getClass)
  val spark = SparkSession.builder()
    .appName("Police Data Save Es")
    .config("spark.sql.warehouse.dir","/usr/hive/warehouse")
    .config("es.nodes","10.179.48.217:9200,10.179.117.160:9200,10.179.40.166:9200")
    .config("pushdown", "true")
    .config("es.nodes.wan.only","true")
    .enableHiveSupport()
    .getOrCreate()
  import spark.sql
  sql("use dim")

  val dic_sql = "select fieldname, valuecode, valuename1 from codes where tablename='db_6he1_bas_police'";
  val dicDf = sql(dic_sql)
  val dic_array = dicDf.rdd.collect()
  val dicMap = dic_array.map(list => (list(0)+list(1).toString, list(2).toString)).toMap
}



