package com.lsy.myhadoop.spark2.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextUtils {
  private var sc:SparkContext = null
  private var hiveContext:HiveContext = null


  def getOrInitSparkContext(appName:String): SparkContext = {
    if(sc == null){
      val sparkConf = new SparkConf()
      sparkConf.setAppName(appName)
      sc = new SparkContext(sparkConf)
    }
    sc
  }

  def getOrInitHiveContext(sc:SparkContext): HiveContext = {
    if(hiveContext == null){
      hiveContext = new HiveContext(sc)
    }
    hiveContext
  }

  def readFileBySc(sc:SparkContext, path:String) = {
    val rdd = sc.textFile(path)
    rdd
  }

  def saveFileForRdd(rdd:RDD[_], path:String) = {
    rdd.saveAsTextFile(path)
  }

  // didi hbase bulkload
//  def readTextFileBySc(sc:SparkContext, path:String, compression:String=""):RDD[String] = {
//    val rdd:RDD[String] =
//      if("lzo" == compression) sc.newAPIHadoopFile(path,
//      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
//      classOf[org.apache.hadoop.io.LongWritable],
//      classOf[org.apache.hadoop.io.Text]).map(_._2.toString)
//      else sc.textFile(path)
//    rdd
//  }

  /**
    * hdfs输出的轨迹数据是lzo压缩的，数据处理时间可能会增长，使用方可以在spark中添加sparkconf.set("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize","67108864")，减少每个split的数据量，提高并发
    */

}
