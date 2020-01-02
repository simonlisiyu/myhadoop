package com.lsy.myhadoop.spark2.beifeng.core.count

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lisiyu on 2016/3/28.
 */
object Median {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("median").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/root/aaa")

    val words = data.flatMap(_.split(" ")).map(word => word.toInt)

    //将数据划分为4个桶并排序，统计每个桶中落入的数据量
    val number = words.map(word =>(word/4,word)).sortByKey()

    // pairCount计算每个分组内的个数
    val pairCount = words.map(word => (word/4,1)).reduceByKey(_+_).sortByKey()

    // count是统计总的个数
    val count = words.count().toInt

    // mid是中位数在整个数据的偏移量
    var mid =0
    if(count%2 != 0)
    {
      mid = count/2+1
    }else
    {
      mid = count/2
    }

    // temp是中位数所在区间累加的个数
    var temp =0

    // temp1是中位数所在区间的前面所有区间累加的个数
    var temp1= 0

    // index是中位数的区间
    var index = 0

    // tongNumber是桶的个数
    val tongNumber = pairCount.count().toInt

    var foundIt = false
    for(i <- 0 to tongNumber-1 if !foundIt)
    {
      //pairCount是一个RDD，不能直接取出第几个值，所以通过collectAsMap转成Scala的Map集合
      temp = temp + pairCount.collectAsMap()(i)
      temp1 = temp - pairCount.collectAsMap()(i)
      if(temp >= mid)
      {
        index = i
        foundIt = true
      }
    }

    // tonginneroffset是中位数在桶中的偏移量
    val tonginneroffset = mid - temp1

    // takeOrdered默认可以将key从小到大排序后，获取RDD中的前n个元素
    val median = number.filter(_._1==index).takeOrdered(tonginneroffset)
//    sc.setLogLevel("ERROR")
    println(median(tonginneroffset-1)._2)
    sc.stop()

  }
}
