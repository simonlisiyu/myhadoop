package com.lsy.myhadoop.spark2.io

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkHiveIO {

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()

    val df = executeSqlFromHive(spark, "mydb", "SELECT * FROM mytable")
    val dff = executeSqlFromHive(spark, "mydb", "INSERT OVERWRITE TABLE mytable PARTITION (city_id=12, dt='20180202")
  }

  /**
    * read hive table, return DataFrame
    * @param spark
    * @param db
    * @param sql
    * @return
    */
  def executeSqlFromHive(spark:SparkSession, db:String, sql:String) = {
    spark.sql(s"use ${db}")
    spark.sql(sql)
  }

  def writeDataFrameToHive(spark:SparkSession, df:DataFrame, saveMode:SaveMode, db:String, table:String) = {
    spark.sql(s"use ${db}")
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict ")
    spark.sql("SET hive.exec.max.dynamic.partitions.pernode = 400")
    df.repartition(10).write.mode(saveMode).insertInto(table)
  }
}
