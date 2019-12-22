package com.lsy.myhadoop.spark2.io

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.NLineInputFormat
import org.apache.hadoop.mapred._
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * spark to hdfs input and output
  */
object SparkFileIO {
  def main(args: Array[String]): Unit = {

    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val listRdd = sc.parallelize(List(1, 2, 3), 3)
    val pets = sc.parallelize(List(("cat", 1), ("dog", 1), ("cat", 2)))

    val ds = readDatasetStrFromHdfs(spark, "/user/lsy/test")
    ds
    val dff = ds.toDF()
    dff

    val save = writeDataFrameToHdfs(spark, ds.toDF(), "/user/lsy/test1")
    save

    // read with format
    val df = spark.read.format("json").option("samplingRatio", "0.1").load("/home/data.json")
//    val df1 = spark.read.json("/home/data.json")
    // write with format
    df.write.format("parquet").mode("append").partitionBy("datetime").saveAsTable("fasterData") //存到hive中，需要环境hive-site.xml
    df.write.format("parquet").mode("append").partitionBy("datetime").save("/home/data.parquet")
    df.write.parquet("/home/data.parquet")
    df.write.mode(SaveMode.Overwrite).json("/tmp/user.json")


      // stop spark session
    spark.stop()
  }

  /**
    * sc read file
    * @param spark
    */
  def scReadFile(spark:SparkSession) = {
    val sc = spark.sparkContext
    sc.textFile("file:///tmp/test.txt", 10)   // local file
    sc.textFile("/tmp/test.txt")          // 取决于spark运行环境（spark-env.sh）是否配置了HADOOP_CONF_DIR
    sc.textFile("hdfs://localhost:9000/tmp/test.txt")
    sc.textFile("hdfs:///tmp/test.txt")

//    sc.sequenceFile("file:///tmp/test.txt")
//    sc.sequenceFile[String, Int]("hdfs://localhost:9000/tmp/test.txt")

    val fileRDD = sc.hadoopFile[LongWritable, Text, TextInputFormat]("/tmp")
    val hadoopRdd = fileRDD.asInstanceOf[HadoopRDD[LongWritable, Text]]



    // 自定义 input format
    // 将文本文件中的N行作为一个input split，由一个Map Task处理。
    // 创建一个JobConf，并设置输入目录及其他属性
    val jobConf = new JobConf()
    FileInputFormat.setInputPaths(jobConf, "/user/data1")
    // 使用NLineInputFormat, 为了与hadoop 1.0 和2.0都兼容，我们同时设置了两个属性
    jobConf.setInt("mapred.line.input.format.linespermap", 100)
    jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 100)
    // key的类型是LongWritable, value类型是Text
    val inputFormatClass = classOf[NLineInputFormat]
    var hdRdd = sc.hadoopRDD(jobConf, inputFormatClass, classOf[LongWritable], classOf[Text])

  }

  /**
    * sc write file
    * @param spark
    * @param rdd
    */
  def scWriteFile(spark:SparkSession, rdd:RDD[Any]) = {
    rdd.saveAsTextFile("hdfs://nn:8020/output")
    rdd.saveAsObjectFile("hdfs://nn:8020/output")

//    val fileRdd = spark.sparkContext.sequenceFile("/tmp/data1")
//    fileRdd.saveAsSequenceFile("/tmp/data2")
  }


  /**
    * read hdfs file, return Dataset[String]
    * @param spark
    * @param path
    * @return
    */
  def readDatasetStrFromHdfs(spark:SparkSession, path:String):Dataset[String] = {

    // hadoop rdd
    val fileRdd = spark.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
    val hadoopRdd = fileRdd.asInstanceOf[HadoopRDD[LongWritable, Text]]
    val junctionFlowDS = hadoopRdd.mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
      val filePath = inputSplit.asInstanceOf[FileSplit]
      iterator.map(line => (filePath.getPath.toString, line._2.toString))
    }).filter(item => parseVersionAndCityId(item._1) match {
      case Some((_: String, cityId: Int)) => true
      case None => false
    })
      .flatMap(item => item._1.split(","))
    junctionFlowDS

    // method 1
    spark.read.textFile(path).repartition(100)
  }


  /**
    * write DataFrame to hdfs
    * @param spark
    * @param df
    * @param path
    */
  def writeDataFrameToHdfs(spark:SparkSession, df:DataFrame, path:String):Unit = {
    import spark.implicits._
    df.map(_.mkString("\t")).rdd.repartition(1).saveAsTextFile(path)
    df.repartition(10).map(_.mkString("\t")).write.mode(SaveMode.Overwrite).text(path)
  }


  implicit def stringToInt(tempString: String): Int = Integer.parseInt(tempString)

  implicit def stringToBigInt(tempString: String): BigInt = BigInt(tempString)

  def parseVersionAndCityId(filePath: String): Option[(String, Int)] = {
    val regex = """.*\/([0-9]{10,10})\/flow_city\/flow_city_([0-9]+)_hdfs""".r
    filePath match {
      case regex(mapVersion, cityId) => Some(Tuple2(mapVersion, cityId))
      case _ => None
    }
  }


}
