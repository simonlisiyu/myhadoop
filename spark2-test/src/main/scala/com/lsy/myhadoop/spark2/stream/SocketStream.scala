package com.lsy.myhadoop.spark2.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val linesWindow = lines.window(Seconds(30), Seconds(2)).flatMap(_.split(" "))
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
