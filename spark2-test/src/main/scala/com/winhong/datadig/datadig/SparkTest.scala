package com.winhong.datadig.datadig

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 11/12/15.
  */
object SparkTest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf
    //conf.setAppName("App").setMaster("spark://Slave-3:7077")
    conf.setAppName("App").setMaster("local")
    val sc = new SparkContext(conf)

//    val textFile = sc.textFile("hdfs://localhost:9000/spark-in/u1.test")
//
//    //extract (movieid, rating)
//    val rating = textFile.map(line => {val fileds = line.split("\t")
//      (fileds(1).toInt, fileds(2).toDouble)})
//    rating.take(10).foreach(println)
//
//
//    //get rating average count per movieid
//    val movieScores = rating.groupByKey().map(data => {val avg = data._2.sum / data._2.size
//      (data._1, avg)})
//
//    // Read movie (movieid|name)
//    val movies = sc.textFile("hdfs://localhost:9000/spark-in/u.item")
//    movies.take(10).foreach(println)
//    println()
//    val movieskey = movies.map(line => {val fileds = line.split("\\|")
//      (fileds(0).toInt, fileds(1))
//    }).keyBy(_._1)
//    movieskey.take(10).foreach(println)
//
//    // by join, we get <movie, moviename="" averagerating,="">
//    val result = movieScores.keyBy(_._1).join(movieskey)
//      .filter(f => f._2._1._2 > 4.0)
//      .map(f => (f._1, f._2._1._2, f._2._2._2))
//    result.foreach(println)


//    val a = sc.parallelize(1 to 9, 3)
//    a.foreach(println)
//    println("rdd-----------------")

    val rdd = sc.parallelize(1 to 9, 3)
    val bRdd = rdd.map(x => x*2)
    bRdd.collect.foreach(println)
    println("map-----------------")


//    val parRdd = sc.parallelize(1 to 9, 3)
//    def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
//      var res = List[(T, T)]()
//      var pre = iter.next while (iter.hasNext) {
//        val cur = iter.next;
//        res .::= (pre, cur) pre = cur;
//      }
//      res.iterator
//    }
//    val partiRdd = parRdd.mapPartitions(myfunc).collect
//    partiRdd.foreach(println)
//    println("mapPartitions-----------------")


    val vaRdd = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2)
    val valueRdd = vaRdd.map(x => (x.length, x))
    val valuesRdd = valueRdd.mapValues("x" + _ + "x").collect
    valuesRdd.foreach(println)
    println("mapValues-----------------")




    val x = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)
    val y = x.mapWith(a => a * 10)((a, b) => (b + 2)).collect
    y.foreach(println)
    println("mapWith-----------------")


    val b = sc.parallelize(1 to 4, 2)
    val c = b.flatMap(x => 0 to x)
    c.foreach(println)
    println("flatMap-----------------")


    val d = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 3)
    val e = d.flatMapWith(x => x, true)((x, y) => List(y, x)).collect
    e.foreach(println)
    println("flatMapWith----------------")


    val f = y ++ e
    f.foreach(println)
    println("rdd ++ -----------------")

    val f2 = y.zip(e)
    f2.foreach(println)
    println("rdd zip-----------------")


    val g = sc.parallelize(List((1,2),(3,4),(3,6)))
    val h = g.flatMapValues(x=>x.to(5))
    h.collect().foreach(println)
    println("flatMapValues-----------------")

    val i = sc.parallelize(1 to 10)
    val j = i.reduce((x, y) => x + y)
    println(j)
    println("reduce-----------------")

    val k = sc.parallelize(List((1,2),(3,4),(3,6)))
    k.foreach(println)

    val l = k.reduceByKey((x,y) => x + y)
    k.foreach{
      m=> println(m._1 +"\t " + m._2)
    }
    println("reduceByKey-----------------")
    val n = k.reduce((a, b) =>{
      (a._1+b._1,a._2+b._2)
    })
    println(n)
    println("reduce ++ -----------------")

    val filterRdd = sc.parallelize(List(1,2,3,4,5)).map(_*2).filter(_>5)
    filterRdd.foreach(println)
    println("filter-----------------")

    val rdd1 = sc.makeRDD(1 to 5,2)
    //rdd1有两个分区
    val rdd2 = rdd1.mapPartitionsWithIndex{
      (x,iter) => {
        val result = List[String]()
        var i = 0
        while(iter.hasNext){
          i += iter.next()
        }
        result.::(x + "|" + i).iterator

      }
    }
    rdd2.foreach(println)
    println("mapPartitionsWithIndex-------")

    val o = sc.parallelize(1 to 9, 3)
    def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
      var res = List[(T, T)]()
      var pre = iter.next
      while (iter.hasNext) {
        val cur = iter.next
        res.::= (pre, cur)
        pre = cur
      }
      res.iterator
    }
    val p = o.mapPartitions(myfunc).collect
    p.foreach(println)
    println("mapPartitions-------")


    sc.stop()
  }
}
