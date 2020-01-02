package com.lsy.myhadoop.spark2.beifeng.sql.count

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lisiyu on 2016/4/9.
 */
object DailyTop3Keyword {

  val keyMap = Map(
    ("city"->List("beijing")),
    ("platform"->List("android")),
    ("version"->List("1.0","1.5","2.0")))

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DailyTop3Keyword")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val broadcastMap = sc.broadcast(keyMap)


    val rawRDD = sc.textFile("hdfs://192.168.190.132:9000/test/keyword.txt")
    val filterRDD = rawRDD.filter(line => {
      val split = line.split("\t")
      if(!broadcastMap.value("beijing").contains(split(3))){
        return false
      }
      if(!broadcastMap.value("platform").contains(split(4))){
        return false
      }
      if(!broadcastMap.value("version").contains(split(5))){
        return false
      }
      true
    })

    val groupedRDD = filterRDD.map(line => {
      val split = line.split("\t")
      val date = split(0)
      val user = split(1)
      val word = split(2)
      (date+"_"+word, user)
    }).distinct().groupByKey()

    val countRDD = groupedRDD.map(tuple => {
      val ite = tuple._2.iterator
      var count = 0
      if(ite.hasNext){
        count += 1
        ite.next()
      }
      (tuple._1, count)
    })

    val rowRDD = countRDD.map(tuple => {
      val date = tuple._1.split("_")(0)
      val word = tuple._1.split("_")(1)
      Row(date, word, tuple._2)
    })

    val structType = StructType(List(
      StructField("date", StringType, true),
      StructField("word", StringType, true),
      StructField("uv", IntegerType, true)
    ))

    val uvDF = hiveContext.createDataFrame(rowRDD, structType)
    uvDF.registerTempTable("uv_table")

    val top3DF = hiveContext.sql("SELECT date,word,uv FROM ("
      +"SELECT date,word,uv,"
      +"row-number OVER (PARTITION BY date ORDER BY uv DESC) rank"
      +"FROM uv_table WHERE rank <= 3)");

    val top3GroupedRDD = top3DF.rdd.map(row => (row.getAs[String]("date"),
      row.getAs[String]("word")+"_"+String.valueOf(row.get(2)))).groupByKey()

    val top3SortedRDD = top3GroupedRDD.map(tuple => {
      var dateWord = tuple._1
      var totalUv = 0
      val ite = tuple._2.iterator
      while(ite.hasNext){
        val wordUv = ite.next()
        val split = wordUv.split("_")
        val word = split(0)
        val uv = Integer.valueOf(split(1))
        totalUv += uv
        dateWord += ","+word
      }
      (totalUv, dateWord)
    }).sortByKey(false)

    val top3RowRDD = top3SortedRDD.flatMap(tuple => {
      val split = tuple._2.split(",")
      val date = split(0)
      Array(Row(date, split(1).split("_")(0), Integer.valueOf(split(1).split("_")(1))),
        Row(date, split(2).split("_")(0), Integer.valueOf(split(2).split("_")(1))),
        Row(date, split(3).split("_")(0), Integer.valueOf(split(3).split("_")(1))))
    })

    val resultDF = hiveContext.createDataFrame(top3RowRDD,structType)

    resultDF.saveAsTable("top3_uv")

    sc.stop()
  }

}
