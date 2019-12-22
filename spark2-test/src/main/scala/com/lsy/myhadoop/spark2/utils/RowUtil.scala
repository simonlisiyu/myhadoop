package com.lsy.myhadoop.spark2.utils

import org.apache.spark.sql.Row

import scala.util.parsing.json.JSONObject

object RowUtil {

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
  }

}
