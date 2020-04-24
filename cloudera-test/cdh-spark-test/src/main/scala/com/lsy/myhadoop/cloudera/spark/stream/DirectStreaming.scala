package com.lsy.myhadoop.cloudera.spark.stream

import com.lsy.myhadoop.cloudera.spark.utils.{ConfUtils, KafkaManager}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lisiyu on 2018/9/8.
  */
object DirectStreaming {

  def  main(args: Array[String]): Unit = {
    //1.param
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("DirectStreaming")
      .setMaster("yarn") //线程数要大于receiver个数
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.receiver.writeAheadLog.enable","true") //表示开启WAL预写日志，保证数据源的可靠性

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./spark_direct") //它会保存topic的偏移量

    val topics = Set("datain")
    val kafkaParams = ConfUtils.loadPropertiesFileToMap("kafka.properties")
    println("kafkaParams===============")
    kafkaParams.foreach(println)

    val manager = new KafkaManager(kafkaParams)

    val dstream = manager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    dstream.print()
    dstream.foreachRDD(rdd => {
      rdd.foreachPartition(printPar)
      manager.updateZKOffsets(rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def printPar(datas: Iterator[(String, String)]) = {
    while (datas != null && datas.hasNext) {
      val n = datas.next()
//      println(n._1)
//      println(n._2)
     println(n)
    }
    println("*** requesting streaming termination")
  }
}
