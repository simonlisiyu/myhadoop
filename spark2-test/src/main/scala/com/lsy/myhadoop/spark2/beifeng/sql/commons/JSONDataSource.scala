package com.lsy.myhadoop.spark2.beifeng.sql.commons

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 4/6/16.
  */
object JSONDataSource {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ParquetMergeSchema")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentScoreDF = sqlContext.read.json("hdfs://192.168.190.132:9000/test/students080.json")

    studentScoreDF.registerTempTable("studentScore")
    val goodStudentScoreDF = sqlContext.sql("select name,score from studentScore where score >= 80")
    val goodStudentNames = goodStudentScoreDF.rdd.map(x => x(0)).collect()

    val studentInfoJson = Array("{\"name\":\"Leo\",\"age\":18}", "{\"name\":\"Marry\",\"age\":17}", "{\"name\":\"Jack\",\"age\":19}")

//    val studentInfoRDD = sc.parallelize(studentInfoJson, 3)
    val studentInfoRDD = sc.parallelize(studentInfoJson)
    val studentInfoDF = sqlContext.read.json(studentInfoRDD)
    studentInfoDF.registerTempTable("studentAge")

    var sql = "select name,age from studentAge where name in ("
    for(name <- goodStudentNames){
      sql = sql + "'" + name + "',"
    }
    sql = sql.substring(0, sql.length-1)
    sql = sql + ")"

    val goodStudentInfoDF = sqlContext.sql(sql)

    val goodStudentJoinedRDD = goodStudentScoreDF.rdd.map(row =>(row.getAs[String]("name"),row.getAs[Long]("score"))).join(
      goodStudentInfoDF.rdd.map(row => (row.getAs[String]("name"),row.getAs[Long]("age"))))

    /*

      goodStudentScoreDF.rdd.map(row => (row[String](0),row[Int](1))).join(
    goodStudentInfoDF.rdd.map(row => (row[String](0),row[Int](1))))
    )*/

    val goodStudentRowRDD = goodStudentJoinedRDD.map(x => Row(x._1, x._2._1.toInt, x._2._2.toInt))

    val structType = StructType(Array(
      StructField("name",StringType,true),
      StructField("score",IntegerType,true),
      StructField("age",IntegerType,true)))

    val goodStudentDF = sqlContext.createDataFrame(goodStudentRowRDD,structType)

    goodStudentDF.write.format("json").save("hdfs://192.168.190.132:9000/test/080")

  }

}
