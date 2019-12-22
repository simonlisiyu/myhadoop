package com.lsy.myhadoop.spark2.compute

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import sun.jvm.hotspot.debugger.cdbg.IntType

case class Person(name: String, age: Long)

case class User(userID: Long, gender: String, age: Int, occupation: String, zipcode: String)

object RDDDFDSConvertApi {

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    // rdd to ds
    val personRDD = sc.makeRDD(Seq(Person("A",10), Person("B",20)))
    personRDD.toDS()
    val personDS = spark.createDataset(personRDD)

    // df to ds
    val dataframe = spark.read.json("people.json")
    val ds:Dataset[Person] = dataframe.as[Person]

    // ds to ...
    // 1
    ds.rdd
    ds.toDF()
    ds.toDF("name", "age")
    ds.toDF().rdd
    // 2
    val wordsDataset = sc.parallelize(Seq("1", "Tom", "36")).toDS()
    val wordsDf = wordsDataset.toDF("id", "name", "age")

    // rdd to df
    val usersRdd = sc.textFile("/tmp/ml-1m/users.dat")
    val userRowRdd = usersRdd.map(_.split("::")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim))
    // 1
    import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
    val schemaString = "userID gender age occupation zipcode"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val userDataFrame2 = spark.createDataFrame(userRowRdd, schema)
    // 2
    val dfSchema = StructType(
//      (NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
//      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
      Array(
        StructField("userID", StringType, true),
        StructField("gender", StringType, true),
        StructField("age", IntegerType, true),
        StructField("occupation", StringType, true),
        StructField("zipcode", IntegerType, true)
      ))
    // Apply the schema to the RDD
    val userDF = spark.createDataFrame(userRowRdd, dfSchema)
    // Creates a temporary view using the DataFrame
    userDF.createOrReplaceTempView("user_table")

  }

}
