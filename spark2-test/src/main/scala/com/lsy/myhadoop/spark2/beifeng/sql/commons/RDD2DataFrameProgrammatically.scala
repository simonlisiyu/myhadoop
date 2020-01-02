package com.lsy.myhadoop.spark2.beifeng.sql.commons

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @author Administrator
 */
object RDD2DataFrameProgrammatically extends App {
  
  val conf = new SparkConf()
      .setMaster("local")  
      .setAppName("RDD2DataFrameProgrammatically")  
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  
  // 第一步，构造出元素为Row的普通RDD
  val studentRDD = sc.textFile("hdfs://192.168.190.132:9000/test/students", 1)
      .map { line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt) } 
  
  // 第二步，编程方式动态构造元数据
  val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))  
  
  // 第三步，进行RDD到DataFrame的转换
  val studentDF = sqlContext.createDataFrame(studentRDD, structType)  
  
  // 继续正常使用
  studentDF.registerTempTable("students")  
  
  val teenagerDF = sqlContext.sql("select * from students where age<=18")  
  
  val teenagerRDD = teenagerDF.rdd.collect().foreach { row => println(row) }    
}