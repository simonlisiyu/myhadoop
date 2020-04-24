/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.lsy.myhadoop.spark2.stream

import java.util.Arrays

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import shaded.parquet.org.apache.thrift.protocol.TBinaryProtocol
import shaded.parquet.org.apache.thrift.transport.TMemoryBuffer
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object DirectKafkaWordCountForDstream {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

//    val directKafkaStream = KafkaUtils.createDirectStream[
//      [key class], [value class], [key decoder class], [value decoder class] ]
//    ( streamingContext, [map of Kafka parameters], [set of topics to consume])

//    directKafkaStream.repartition(M).map { ....}.foreachRDD {...}

    // 创建DStream，接收kafka数据
    val inputDStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](ssc, kafkaParams, topicsSet, "dstreamid")
    val lines = inputDStream.map(_.value)

    // window data
//    val linesWindow = inputDStream.window(Seconds(30), Seconds(2)).map(_.value)

//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc,
//      kafkaParams,
//      topicsSet)

    // 数据 produce
    lines.foreachRDD((rdd, time) => {
      rdd.foreachPartition(write)
      //业务处理完之后提交offset。commitOffset第一个参数为DstreamID，第二个参数为当前批次时间
      KafkaUtils.commitOffset(Array("dstreamid"), time)
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def write(datas: Iterator[Array[Byte]]): Unit = {
    var dataByte: Array[Byte] = null

    while (datas != null && datas.hasNext) {
      try {
        dataByte = datas.next()
        val tmb = new TMemoryBuffer(dataByte.length)
        tmb.write(dataByte)
        val oprot = new TBinaryProtocol(tmb)
//        val request = new MapMatchRes2SpdCalcRequest
//        request.read(oprot)
//        val list = request.getMap_match_point_vec.asScala
//        //println("##############" + request.toString)
//        val updateTimeStamp = request.getTimestamp
//        list.map {
//          line => {
//            val mmp = new MapMatchPoint()
//            mmp.setMap_match_point(line)
//            mmp.setUpdate_timestamp(updateTimeStamp.toString)
//            val mb = new TMemoryBuffer(160)
//            val proto = new TBinaryProtocol(mb)
//            mmp.write(proto)
//            // message中的经纬度(proj_x:11380121, proj_y:2303883),所以要除以100000
//            val rowKey = StrUtils.rowKeyPrefix(line.getProj_x / 100000.0, line.getProj_y / 100000.0) + line.getTimestamp.toString
//            // hbaseTable.write(rowKey, HbaseTableConstant.COLUMN_FAMILY, line.getUser_id, mb.getArray)
//            // 尽量减少无效字符存入HBase
//            hbaseTable.write(rowKey, HbaseTableConstant.COLUMN_FAMILY, line.getUser_id, Arrays.copyOf(mb.getArray, mb.length()))
//          }
        }
      }
//      catch {
//        case e: Exception =>
//          logger.error(s"HbaseWiter write errror!!! data is: ${dataByte}", e)
//          e.printStackTrace()
//      }
  }
}


