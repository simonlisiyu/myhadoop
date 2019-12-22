package com.lsy.myhadoop.hbasescala.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by lisiyu on 2018/5/8.
  */
object HBaseUtils {

  /**
    * 初始化hbase table
    * @param zkQuorum
    * @param zkPath
    * @param tableName
    * @param autoFlush
    * @param clearBufferOnFail
    * @param writeBufferSize
    * @return
    */
  def initHTable(zkQuorum:String, zkPath:String, tableName:String, tmpDir:String,
                 autoFlush:Boolean=true, clearBufferOnFail:Boolean=true,
                 writeBufferSize:Long=5*1024*1024): HTable = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
//    conf.set("hbase.zookeeper.property.clientPort", zkClientPort)
    conf.set("zookeeper.znode.parent", zkPath)
    conf.set("hbase.defaults.for.version.skip", "true") //是否跳过hbase.defaults.for.version的检查，默认是false
    conf.set("hbase.fs.tmp.dir", tmpDir)
    val myTable = new HTable(conf, TableName.valueOf(tableName))
    myTable.setAutoFlush(autoFlush, clearBufferOnFail)  //自动提交与fail清理buffer，默认为true,true
    myTable.setWriteBufferSize(writeBufferSize) //hbase.client.write.buffer，决定自动提交的批次数据大小
    myTable
  }
  def initHTable(conf: Configuration, tableName:String): HTable = {
    val myTable = new HTable(conf, TableName.valueOf(tableName))
    myTable
  }

  /**
    * 初始化hbase conf
    * @param zkQuorum
    * @param zkPath
    */
  def initHConf(zkQuorum:String, tmpDir:String, zkPath:String="/hbase", zkPort:String="2181"): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    conf.set("zookeeper.znode.parent", zkPath)
    conf.set("hbase.defaults.for.version.skip", "true") //是否跳过hbase.defaults.for.version的检查，默认是false
    conf.set("hbase.fs.tmp.dir", tmpDir)
    conf
  }

  /**
    * 将一行数据写入到hbase table的buffer中
    * @param rowkey
    * @param cf
    * @param column
    * @param value
    * @param table
    */
  def putRowToTable(rowkey:String, cf:String, column:String, value:String, table:HTable) = {
    val p = new Put(Bytes.toBytes(rowkey))
    p.add(cf.getBytes, column.getBytes, Bytes.toBytes(value))
    table.put(p)
  }

  /**
    * 主动提交hbase table的buffer数据
    * @param table
    */
  def flushToTable(table:HTable) = {
    table.flushCommits()
  }


}
