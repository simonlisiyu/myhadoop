package com.lsy.myhadoop.spark2.io

import org.apache.spark.sql.SparkSession

import scala.actors.threadpool.{Callable, Executors}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


object SparkFutureIO {

  def main(args: Array[String]): Unit = {
    val jobName = "future_test"

    val spark = SparkSession.builder().appName(s"${jobName}")
      .enableHiveSupport().getOrCreate()

    val dirs = Set("111", "222")
    val path = "/user/lsy/test/"

    val futures = dirs.map(dir => Future {
      val lines = spark.read.textFile(path + dir)
      import spark.implicits._
      val data = lines.map(line => Tuple1(line))
      dir
    })

    val result = Future.sequence(futures)
    Await.result(result, Duration.Inf)
    result.foreach(item => item.foreach(println))
    spark.stop()
  }

  def mains(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("app").getOrCreate()
    val sc = spark.sparkContext

    val dirs = Set(
      "/Users/lsy/Softwares/bigdata/data/test-data/ml-100k/u.user",
      "/Users/lsy/Softwares/bigdata/data/test-data/ml-100k/u.data")

    val executorService = Executors.newFixedThreadPool(3)
    val futures = dirs.map(dir => executorService.submit(new Callable[Long]() {
      @Override
      def call: Long = {
        val rdd = sc.textFile(dir)
        return rdd.count()
      }
    }))
    futures.foreach(future => println(future.get()))
    spark.stop()
  }


}
