package com.lsy.myhadoop.spark2.io

import org.apache.spark.sql.SparkSession

object SparkJdbcIO {

  def main(args: Array[String]): Unit = {

    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val jdbcDF = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:postgresql:dbserver", "dbtable" -> "schema.tablename")).load()

    // export SPARK_CLASSPATH=<mysql-connector-java-5.1.26.jar>
    val jdbcDF2 = spark.read.format("jdbc").options(Map(
    "url" -> "jdbc:mysql://mysql_hostname:mysql_port/testDB", "dbtable" -> "testTable")).load()

    " CREATE TABLE user USING jdbc OPTIONS(“jdbc:mysql://mysql_hostname:mysql_port/testDB”, “dbtable” -> “testTable”)"
  }

}
