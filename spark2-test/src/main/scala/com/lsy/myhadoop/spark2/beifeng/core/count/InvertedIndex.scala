package com.lsy.myhadoop.spark2.beifeng.core.count

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by lisiyu on 2016/3/28.
 */
object InvertedIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("invertedIndex").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/root")
    val wordMapId = lines.map(x =>{
      val id = x.split(" ")(0)
      val arrBuff = ArrayBuffer(x.split(" "))
      arrBuff.remove(0,1)
      var word = ""
      for(i <- 0 to arrBuff.length){
        word += arrBuff(i)+" "
      }
      (word.substring(0,word.length-1), id)
    }).flatMap(x => {
      var map = scala.collection.mutable.Map[String,String]()
      val words = x._1.split(" ").iterator
      while (words.hasNext){
        map += (words.next() -> x._2)
      }
      map

    }).distinct()

    //save to file
    wordMapId.reduceByKey(_+" "+_).map(x=>{
      x._1+"\t"+x._2
    }).saveAsTextFile("inverted_index")



    sc.stop()
  }

}
