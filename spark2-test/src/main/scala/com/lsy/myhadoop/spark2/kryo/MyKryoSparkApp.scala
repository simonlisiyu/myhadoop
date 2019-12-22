package com.lsy.myhadoop.spark2.kryo

import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
  * Protobuf
  */
object MyKryoSparkApp {

  def main(args: Array[String]): Unit = {
    println(System.currentTimeMillis() +"kryo spark task start")

    if (args.length < 2) {
      System.err.println(s"Usage: ${getClass.getName} <a> <b> <c>")
      System.exit(1)
    }

    // 2. init spark session and context
    //    hdfs
    val conf = new SparkConf()
//    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")  // new exception: Class not found
    conf.set("spark.kryo.registrator", "com.lsy.knowledge.traffic.base.domain.protoc.MyKryoRegistrator") // worked
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")   // new
//    conf.registerKryoClasses(Array(classOf[TrajPb.OrderTraj], classOf[TrajPb.map_match_point_pb]));
    val spark = SparkSession.builder().config(conf).appName("jobname").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext


    spark.stop()
    System.exit(0)

  }

  /**
    * hadoop distcp -Dmapreduce.job.queuename=root.123.lsy -m 5 -bandwidth 50 /user/bi/lisiyu/hbase/DMP/ORDER/${cityId}/${year}/${month}/${day} hdfs://cluster:8020/user/lsy/bulkload/DMP/ORDER/
    * hadoop distcp -Dmapreduce.job.queuename=root.123.lsy -m 5 -bandwidth 50 /user/bi/lisiyu/hbase/DMP/LORDER/${cityId}/${year}/${month}/${day} hdfs://cluster:8020/user/lsy/bulkload/DMP/LORDER/
    */



}

