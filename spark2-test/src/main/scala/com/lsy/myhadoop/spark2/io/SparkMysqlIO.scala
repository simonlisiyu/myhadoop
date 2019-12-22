package com.lsy.myhadoop.spark2.io

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class TestTable(id: String,
                          age: Int,
                          name: String
                       )

object SparkMysqlIO {
  def main(args: Array[String]): Unit = {
    val city_id = 12
    val dateStr = "20180202"

    val config = ConfigFactory.load()

    val spark = SparkSession.builder().appName(s"MysqlTestApp-${city_id}-${dateStr}").enableHiveSupport().getOrCreate()
    val database = config.getString("hive.database")
    spark.sql(s"use ${database}")
    val dataDF = spark.sql(s"SELECT * FROM test_table WHERE city_id = ${city_id} AND dt = '${dateStr}'")
    println(dataDF.count())

    /**
      * http://slick.lightbend.com/doc/3.2.0/sql-to-slick.html
      */
    // delete
    val db = Database.forConfig("mysql")
    val table = "mysql-table"
    val sql = sqlu"delete from #${table} where city_id = #${city_id} AND day = '#${dateStr}'"
    val deleteResult = Await.result(db.run(sql), Duration.Inf)
    println(s"delete ${deleteResult} row numbers")

    // query
    val ids = "'111','222'"
    val query_sql = sql"SELECT a.id,a.age,a.name from #${table} a where id in #${ids}"
      .as[(String,Int,String)]
    val mysqlData = Await.result(db.run(query_sql), Duration.Inf)
    mysqlData.foreach(println)

    dataDF
      .withColumnRenamed("cityId", "city_id")
      .withColumnRenamed("time_slice","hour")
      .withColumnRenamed("dt", "day")
      .write.mode(SaveMode.Append)
      .format("jdbc")
      .option("url", config.getString("mysql.url"))
      .option("dbtable", config.getString("city_table"))
      .option("user", config.getString("mysql.user"))
      .option("password", config.getString("mysql.password"))
      .save()

    spark.stop()
  }

}
