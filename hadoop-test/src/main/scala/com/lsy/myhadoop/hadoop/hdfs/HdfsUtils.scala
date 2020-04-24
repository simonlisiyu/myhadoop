package com.lsy.myhadoop.hadoop.hdfs

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

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

  def downloadFile(fs: FileSystem, hdfsDir:String, localDir:String): Unit = {
    val input = fs.open(new Path(hdfsDir))
    val out = new FileOutputStream(localDir)
    IOUtils.copyBytes(input, out, 1024)
    input.close()
    out.close()
  }

  def uploadFile(fs: FileSystem, localDir:String, hdfsDir:String): Unit = {
    val file = new File(localDir)
    val input = new FileInputStream(file)
    val out = fs.create(new Path(hdfsDir))
    IOUtils.copyBytes(input, out, 1024)
    input.close()
    out.close()
  }

}
