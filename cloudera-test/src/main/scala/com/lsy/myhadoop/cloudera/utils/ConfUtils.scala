package com.lsy.myhadoop.cloudera.utils

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * 配置文件 properties 读取
  */
object ConfUtils {

  private val logger: Logger = LoggerFactory.getLogger(ConfUtils.getClass)

  def loadPropertiesFile(fileName: String): Properties = {
    val ins = ConfUtils.getClass.getClassLoader.getResourceAsStream(fileName)
    val properties = new Properties()
    try {
      properties.load(ins)
    } catch {
      case e: Exception =>
        logger.error("loadPropertiesFile faild! fileName is:" + fileName)
        throw new Exception("loadPropertiesFile faild! fileName is:" + fileName, e)
    } finally {
      ins.close()
    }
    properties
  }

  def loadPropertiesFileToMap(fileName: String): Map[String, String] = {
    val properties = loadPropertiesFile(fileName)
    properties.stringPropertyNames().asScala.map(k => (k, properties.getProperty(k).trim)).toMap
  }

}
