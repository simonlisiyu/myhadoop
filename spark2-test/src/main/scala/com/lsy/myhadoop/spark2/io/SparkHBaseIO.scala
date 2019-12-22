package com.lsy.myhadoop.spark2.io

import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

object SparkHBaseIO {

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").getOrCreate()
    val sc = spark.sparkContext
    import org.apache.hadoop.hbase.mapreduce.TableInputFormat
    import org.apache.hadoop.fs.Path
    import org.apache.hadoop.hbase.HBaseConfiguration
    // 设置hbase configuration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource(new Path("hbase-site.xml"))
//    hbaseConf.set("hbase.zookeeper.quorum", "localhost:2181,localhost:2181")

    hbaseConf.set(TableInputFormat.INPUT_TABLE, "table_name")

    //创建hbase RDD
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    //获取总行数
    val count = hBaseRDD.count()

    //遍历输出
    hBaseRDD.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Gender:"+gender+" Age:"+age)
    })


    val tablename = "student"
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    import org.apache.hadoop.mapreduce.Job
    import org.apache.hadoop.hbase.client.Result
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26","4,Guanhua,M,27")) //构建两行记录
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0))) //行健的值
      put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))  //info:name列的值
      put.add(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))  //info:gender列的值
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3).toInt))  //info:age列的值
      (new ImmutableBytesWritable, put)
    }}
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())

    //    val rdd = sc.sequenceFile("/tmp/data1").map(_ => RDD(("metric", 1231234124424L), 10))
//    rdd.foreach(line => saveToHBase(line, hbaseConf, "table_name"))

  }


  def saveToHBase(rdd : RDD[((String, Long), Int)], conf: HBaseConfiguration, tableName : String) = {
    val jobConfig = new JobConf(conf)
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    new PairRDDFunctions(rdd.map { case ((metricId, timestamp), value) =>
      createHBaseRow(metricId, timestamp, value) }).saveAsHadoopDataset(jobConfig)
  }

  def createHBaseRow(metricId : String, timestamp : Long, value : Int) = {
    val record = new Put(Bytes.toBytes(metricId + "~" + timestamp))
    record.add(Bytes.toBytes("metric"), Bytes.toBytes("col"),
    Bytes.toBytes(value.toString))
    (new ImmutableBytesWritable, record)
  }

}
