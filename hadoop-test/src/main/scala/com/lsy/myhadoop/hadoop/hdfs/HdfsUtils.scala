package com.lsy.myhadoop.hadoop.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by lisiyu on 2018/5/17.
  */
object HadoopUtils {

  def initHConf(): Configuration = {
    val conf: Configuration = new Configuration
    conf
  }

  def initFs(conf: Configuration): FileSystem = {
    val fs = FileSystem.get(conf)
    fs
  }

  def isFile(fs: FileSystem, filename:String): Boolean = {
    val booleanRes = fs.isFile(new Path(filename))
    booleanRes
  }

  def isDir(fs: FileSystem, dir:String): Boolean = {
    val booleanRes = fs.isDirectory(new Path(dir))
    booleanRes
  }

  def isExist(fs: FileSystem, path:String): Boolean = {
    val booleanRes = fs.exists(new Path(path))
    booleanRes
  }

}
