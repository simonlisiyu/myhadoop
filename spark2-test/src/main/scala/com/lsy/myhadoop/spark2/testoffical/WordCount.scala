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
package com.lsy.myhadoop.spark2.testoffical

import org.apache.spark._
import SparkContext._

object WordCount {

  def main(args: Array[String]) {
    if (args.length != 3 ){
      println("usage is org.test.WordCount <master> <input> <output>")
      return
    }
    val sparkConf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rowRdd= sc.textFile(args(1))
    val resultRdd = rowRdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1)).reduceByKey(_ + _)
    resultRdd.saveAsTextFile(args(2))

  }
}

