package com.lsy.myhadoop.spark2.io

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkThirdPartyIO {

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    // third party

    // Github: https://github.com/databricks/spark-csv
    // Maven: com.databricks:spark-csv_2.10:1.2.0
    val userCsvDF = spark.read.format("com.databricks.spark.csv").load("/tmp/user.csv").toDF("userID", "age")

    // Github: https://github.com/databricks/spark-avro
    // Maven: com.databricks:spark-avro_2.10:2.0.1
    val avroDf = spark.read.format("com.databricks.spark.avro").load("/tmp/user.avro")

    // Github: https://github.com/datastax/spark-cassandra-connector
    // Maven: com.datastax.spark:spark-cassandra- connector_2.10:1.5.0-M1
    avroDf.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace"->"reco","table"->"users")).save()

  }

}
