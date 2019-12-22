package com.lsy.myhadoop.spark2.compute

import org.apache.spark.sql.SparkSession

/**
  * 高效分发大对象，比如字典(map)，集合(set)等，每个executor一份，而不是每个task一份;
  * 每个 task 是一个线程，而且同在一个进程运行 tasks 都属于同一个 application。因此每个节点（executor）上放一份就可以被所有 task 共享。
  * broadcast 是 只读的变量
  * 包括HttpBroadcast和TorrentBroadcast两种
  */
object SparkBroadcastApi {

  // Broadcast variables allow the programmer to keep a read-only variable cached on each machine
  // rather than shipping a copy of it with tasks. They can be used, for example,
  // to give every node a copy of a large input dataset in an efficient manner.
  // Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val dataSet = Set(1,2,4,8,10) //假设大小128mb
    val bdata = sc.broadcast(dataSet)
    val rdd = sc.parallelize(1 to 1000*1000, 100)
    val observedSizes = rdd.map(_ => bdata.value.size)

  }

}
