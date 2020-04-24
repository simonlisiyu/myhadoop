package com.lsy.myhadoop.cloudera.spark.sql

import com.lsy.myhadoop.cloudera.spark.utils.CommonUtil
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object HiveSqlJob {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Input params are {}", args.mkString("\t"))
    if (args.length < 3) {
      System.err.println(s"Usage: ${getClass.getName} <tablein> <tableout> <YYYY-MM-DD>")
      System.exit(1)
    }

    val table = args(0)
    val table_out = args(1)
    val dateString = args(2).replace("-", "")
    val (year, month, day) = CommonUtil.parseDateInfo(dateString)

    val jobName = "HIVE_SQL_JOB"
    val config = ConfigFactory.load()
    val database = config.getString("hive.ods.db")

    logger.info(s"database: $database, run days: $dateString")
    val spark = SparkSession.builder().appName(s"$jobName-$dateString").enableHiveSupport().getOrCreate()

    spark.sql(s"use $database")

    val stepOneV1Sql =
      s"""
         |SELECT k1,k2,k3,dt,batch
         |FROM $table
         |WHERE dt = "202004"
         |    AND batch = 123123
         """.stripMargin
    logger.info("$table info: start")
    logger.info(stepOneV1Sql)
    logger.info("$table info: end")
    val stepOneVOneTable = spark.sql(stepOneV1Sql)
    stepOneVOneTable.createOrReplaceTempView(s"tmp_${jobName}_step1v1_${year}_${month}_${day}")

    val stepTwoSql =
      s"""
         |INSERT OVERWRITE TABLE $table_out
         |SELECT k1,k2,k3
         |From tmp_${jobName}_step1v1_${year}_${month}_${day}
       """.stripMargin

    logger.info("save $table_out: start")
    logger.info(stepTwoSql)
    logger.info("save $table_out: end")
    spark.sql(stepTwoSql).show()
    spark.stop()
  }

}
