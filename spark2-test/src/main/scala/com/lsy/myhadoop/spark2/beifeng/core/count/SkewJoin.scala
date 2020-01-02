package com.lsy.myhadoop.spark2.beifeng.core.count

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lisiyu on 2016/3/28.
 */
object SkewJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("skewjoin").setMaster("local")
    val sc = new SparkContext(conf)

    //数据倾斜的表A
    val table1 = sc.textFile("/table1").map { line =>
      val field = line.split("\t")
      val card = field(0)
      val mid = field(1)
      val count = field(2)
      (card, (mid, count))
    }
    //需要连接的表B
    val table2 = sc.textFile("/table2").map(x => (x, x))

    //对数据倾斜的表A进行采样，假设只有一个Key倾斜最严重，获取倾斜最大的key
    val infoSample = table1.sample(false, 0.1, 9).map(x => (x._1, 1)).reduceByKey(_ + _)
    // 获得产生数据倾斜的key
    val maxRowKey = infoSample.sortByKey(false).take(1).toSeq(0)._1

    // 将产生数据倾斜的表进行分割，切分成2张表，一个表存的是发生数据倾斜的key的数据，还有个表是除了该key的数据
    val skewedTable = table1.filter(_._1 == maxRowKey)
    val mainTable = table1.filter(_._1 != maxRowKey)

    val rs = sc.union(table2.join(skewedTable), table2.join(mainTable)).map(line => {
      (line._1, line._2._2._1, line._2._2._2)
    })
    rs.saveAsTextFile(args(2))

  }

}
