//package com.lsy.myhadoop.hbasescala.utils
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.KeyValue
//import org.apache.hadoop.hbase.client.HTable
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.rdd.RDD
//
///**
//  * Created by lisiyu on 2018/5/9.
//  * didi pom
//<hbase.version>0.98.21-hadoop2-ddh-0.1.0</hbase.version>
//  */
//object HBaseBulkLoadUtils {
//
//  /**
//    * spark rdd 生成 hbase bulkload 要求的 hfile 格式数据，写入到hdfs中
//    * @param hbaseConf
//    * @param tableName
//    * @param family
//    * @param column
//    * @param kvRDD
//    * @param output
//    */
//  def writeRDDWithHFileFormat[T](hbaseConf: Configuration, tableName: String, family: String, column: String,
//                              kvRDD: RDD[(String, T)], output: String): Unit = {
//    val orderTable = HBaseUtils.initHTable(hbaseConf, tableName)
//    val orderRegionStartKeyArr = tableRegionStartKeys(orderTable)
//    val orderKVRDD = kvRDD.map(traj => {
//      val rowkey = traj._1
//      val resultKv = makeResultKV(rowkey, family, column, traj._2)
//      resultKv
//    }).repartitionAndSortWithinPartitions(new HFilePartitioner(orderRegionStartKeyArr))
//      .map(x => (x._2.k,x._2.v))//（ImmutableBytesWritable ，KeyValue）
//    writeKVRDDToHdfs(orderKVRDD, hbaseConf, orderTable, output)
//  }
//
//  /**
//    * 拿到 hbase table 所有region的startKey
//    * @param table
//    * @return
//    */
//  private def tableRegionStartKeys(table:HTable): Array[String] = {
//    val startkey_size= table.getStartKeys().size
//
//    var startkeys=new Array[String](startkey_size)
//    var i=0
//    for(row <-  table.getStartKeys()){
//      val startkey = Bytes.toStringBinary(row)
//      startkeys(i)=Bytes.toStringBinary(row)
//      i=i+1
//    }
//
//    startkeys
//  }
//
//  /**
//    * 生成一条 hbase result kv
//    * @param rowKey
//    * @param family
//    * @param column
//    * @param value
//    * @return
//    */
//  private def makeResultKV(rowKey:String, family:String, column:String, value:Any): (String, ResultKV) = {
//    val kv = makeKV(rowKey, family, column, value)
//    (rowKey+"_"+column, kv)
//  }
//  private def makeKV(rowKey:String, family:String, column:String, value:Any): ResultKV = {
//    value match {
//      case valueStr:String => new ResultKV(
//        new ImmutableBytesWritable(rowKey.getBytes()),
//        new KeyValue(rowKey.getBytes(),family.getBytes(), column.getBytes(), valueStr.getBytes()))
////      case valueMMPList:MapMatchPointList => {
////        val mb = new TMemoryBuffer(160)
////        val proto = new TBinaryProtocol(mb)
////        valueMMPList.write(proto)
////        new ResultKV(
////          new ImmutableBytesWritable(rowKey.getBytes()),
////          new KeyValue(rowKey.getBytes(),family.getBytes(), column.getBytes(), Arrays.copyOf(mb.getArray, mb.length())))
////      }
////      case orderTraj:TrajPb => {
////        val mb = new TMemoryBuffer(160)
////        val proto = new TBinaryProtocol(mb)
////        valueMMPList.write(proto)
////        new ResultKV(
////          new ImmutableBytesWritable(rowKey.getBytes()),
////          new KeyValue(rowKey.getBytes(),family.getBytes(), column.getBytes(), Arrays.copyOf(mb.getArray, mb.length())))
////      }
////      case _ => throw new IllegalArgumentException
//      case unexpected => throw new IllegalArgumentException("case unexpected is "+unexpected.toString)
//    }
//  }
//  case class ResultKV(k:ImmutableBytesWritable,v:KeyValue)
//
//  /**
//    * 输出hbase blukload的数据到hdfs
//    * @param rdd
//    * @param conf
//    * @param htable
//    * @param outPath
//    */
//  private def writeKVRDDToHdfs(rdd:RDD[(ImmutableBytesWritable,KeyValue)],
//                       conf:Configuration, htable:HTable, outPath:String) = {
//    val job = Job.getInstance(conf)
//    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
//    job.setMapOutputValueClass(classOf[KeyValue])
//    HFileOutputFormat2.configureIncrementalLoad(job,htable)
//    rdd.saveAsNewAPIHadoopFile(outPath,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],conf)
//
//  }
//
//  /**
//    *
//    * 自定义partition 根据 region的split的分布，将数据分到不同的partition中
//    *
//    * @param startkeyArr 表的每个region startKey
//    */
//  private class HFilePartitioner(startkeyArr: Array[String]) extends Partitioner {
//
//    override def numPartitions: Int = startkeyArr.length
//    override def getPartition(key: Any): Int = {
//      val domain = key.asInstanceOf[String]
//
//      for(i <- 0 until startkeyArr.length){
//        if(domain.toString().compare(startkeyArr(i))<0){
//          return i-1
//        }
//      }
//      //default return 1
//      return startkeyArr.length-1
//    }
//
//    override def equals(other: Any): Boolean = other match {
//      case h: HFilePartitioner =>
//        h.numPartitions == numPartitions
//      case _ =>
//        false
//    }
//  }
//
//
//}
