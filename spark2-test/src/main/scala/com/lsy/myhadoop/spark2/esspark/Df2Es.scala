package com.lsy.myhadoop.spark2.esspark

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by lisiyu on 2020/4/26.
  *
  * <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch-spark-20_2.11</artifactId>
            <version>6.0.0</version>
        </dependency>
  */
class Df2Es {
  val esIndex = "index"
  val esType = "type"
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
    import spark.implicits._
    import spark.sql
    sql("use test")
    val data = sql("select * from test")
    try{
      EsSparkSQL.saveToEs(data,esIndex+"/"+esType)
//      EsSparkSQL.saveToEs(data,esIndex+"/"+esType,Map("es.mapping.id" -> "id")) //指定数据id

      /**
        * es.update.script.inline	ctx._source.location means to update or create a field called location. ctx_source is the ES object to do that.
        * es.update.script.params	location:<Cambridge> are the parameter values passed to the inline script es.update.script.inline. The <> means to write a literal. If we wanted to write a field value we would leave them off.
        * es.mapping.id	This tells ES to look in the dataframe for the id column and use that as the document ID _id. ES uses that to find the document we want to update in ES.
        * es.write.operation	upsert means to add the document if it does not exist, otherwise update it. update means to update it.
            .set("es.net.http.auth.user",user)
            .set("es.net.http.auth.pass",password)
            .set("es.write.operation", "update")
        */
    }catch {
      case es: Exception => {
        print(es)
      }
    }
  }

}
