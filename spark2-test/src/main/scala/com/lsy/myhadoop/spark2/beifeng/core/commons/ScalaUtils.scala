/*
/*################################################################################
//#
//# Name : ScalaUtils.scala
//# Desc : GraphX Gzzzy 定义广东省中医院的Util类 For Scala
//# (C) COPYRIGHT  Corporation 2015
//# All Rights Reserved.
//# Author: John
//# Licensed Materials-Property of 云宏科技
//#
//################################################################################
*/
package spark.study.scala.core.commons

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.log4j.{Level, Logger}

/**
 * Contains additional functionality for [[ScalaUtils]]. All operations are expressed in terms of the
 * efficient GraphX API. This class is implicitly constructed for each Graph object.
 *
 */

class ScalaUtils extends Serializable {

}

object ScalaUtils extends Serializable {

  //define total the same words and words id
  private var totalVertexSameNames : String = ""
  private var totalVertexSameIds : String = ""

  /**
   * Clean all the running infor logs on the console
   *
   * @return this function has no return values
   */
  def cleanRunningLevelInforLogs(warning: Level, off:Level){
    Logger.getLogger("org.apache.spark").setLevel(warning)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(off)

  }

  /**
   * Clean all the total the same word and words information on GraphXLab Scala
   *
   * @return this function has no return values
   */
  def cleanTotalSameWords(){
    this.totalVertexSameIds = ""
    this.totalVertexSameNames = ""
  }

  /**
   * Get final the same words for GraphVDpoint information based on Graph Model
   *
   * @note This function mainly to construct data model for graphx
   * graphs where high degree vertices may force a large amount of
   * information to be collected to a single location.
   *
   * @param  vertextId : VertextID column for Graph Model
   * @param  map:Map[Int,String] : Map information for all the same words based on Graph Model
   *
   * @return return final recursive graphvdpoint information
   */
  def recursiveGraphVDpoint(vertextId : Long, map:Map[Long,String], vRel : String): String = {
    if(map.contains(vertextId)){
      val keyM = map(vertextId)
      if(keyM !="" && keyM.length >0) {
        val sRelation = keyM.split(GraphConstants.vertextFlagShu)
        for (s <- sRelation) {
          if(s!="" && s.length >0) {
            var vertextRelation = ""

            val vertextId = s.split(GraphConstants.vertextMaoHao)(0)
            val vertextName = s.split(GraphConstants.vertextMaoHao)(1)
            //try{
            //vertextRelation = s.split(GraphConstants.vertextMaoHao)(3)
            vertextRelation = s.substring(s.lastIndexOf(GraphConstants.vertextMaoHao)+1,s.length)
            if (vertextRelation == vRel) {
              if (GraphConstants.vertextBlankFor.concat(totalVertexSameIds).
                contains(GraphConstants.vertextBlankFor + vertextId + GraphConstants.vertextBlankFor)) {
                println(GraphConstants.vertextBlank)
              } else {
                totalVertexSameIds = totalVertexSameIds + vertextId + GraphConstants.vertextBlankFor
                totalVertexSameNames = totalVertexSameNames + vertextName + GraphConstants.vertextBlankFor
                recursiveGraphVDpoint(vertextId.toLong, map, vRel)
              }
            }
          }
         /* }catch{
            case e: Exception => e.getMessage
          }finally{}*/
        } //end for (s <- sRelation) {
      }//end if(keyM !="" && keyM.length >0) {
    } //end if(keyM != "" && keyM.length >0) {
    //totalVertexSameNames
    val keepResult = getDestinationValuesBySeparator(totalVertexSameIds, GraphConstants.vertextBlankFor, GraphConstants.vertextFlag).concat(
      getDestinationValuesBySeparator(totalVertexSameNames, GraphConstants.vertextBlankFor, GraphConstants.vertextBlank)
    )
    //println(keepResult)
    keepResult
  }



  /**
   * Get final the same words for GraphVDpoint information based on Graph Model
   *
   * @note This function mainly to construct data model for graphx
   * graphs where high degree vertices may force a large amount of
   * information to be collected to a single location.
   *
   * @param  vertextId : VertextID column for Graph Model
   * @param  map:Map[Int,String] : Map information for all the same words based on Graph Model
   *
   * @return return final recursive graphvdpoint information
   */
  def recursiveGraphVDVertexpoint(vertextId : Int, map:Map[Int,String], vRel : String): String = {
    if(map.contains(vertextId)){
      val keyM = map(vertextId)
      val sRelation = keyM.split(GraphConstants.vertextFlagShu)
      for (s <- sRelation) {
        val vertextId = s.split(GraphConstants.vertextMaoHao)(0)
        val vertextName = s.split(GraphConstants.vertextMaoHao)(1)
        val vertextRelation = s.split(GraphConstants.vertextMaoHao)(3)
        if(vertextRelation == vRel) {
          if (GraphConstants.vertextDot.concat(totalVertexSameIds).
            contains(GraphConstants.vertextDot + vertextId + GraphConstants.vertextDot)) {
            println(GraphConstants.vertextBlank)
          } else {
            totalVertexSameIds = totalVertexSameIds + vertextId + GraphConstants.vertextDot
            totalVertexSameNames = totalVertexSameNames + vertextName + GraphConstants.vertextBlankFor
            recursiveGraphVDVertexpoint(vertextId.toInt, map,vRel)
          }
        }
      } //end for (s <- sRelation) {
    } //end if(keyM != "" && keyM.length >0) {
    //totalVertexSameNames
    getDestinationValuesBySeparator(totalVertexSameNames, GraphConstants.vertextBlankFor, GraphConstants.vertextBlank)
  }


  /**
   * Get destination string by separator
   *
   * @param  inputValues : need to handle with string values for new graph model
   * @param  separator :  seperator this inputvalues
   * @param  appendSeparator : need to append this seprator to the last of privous inputvalues
   * @return this function has no return values
   */
  def getDestinationValuesBySeparator(inputValues:String, separator:String, appendSeparator:String):String = {
    if(inputValues != "" && inputValues !=null && inputValues.length >0) {
      inputValues.substring(0, inputValues.lastIndexOf(separator)).concat(appendSeparator)
    }else{
      GraphConstants.vertextBlank
    }
  }

  /**
   * Get destination string by separator
   *
   * @param  inputValues : need to handle with string values for new graph model
   * @param  separator :  seperator this inputvalues
   * @param  appendSeparator : need to append this seprator to the last of privous inputvalues
   * @return this function has no return values
   */
  def getDestinationValuesBySelfSeparator(inputValues:String, separator:String, appendSeparator:String):String = {
    if(inputValues != "" && inputValues != null && inputValues.length >0) {
      inputValues.substring(0, inputValues.lastIndexOf(separator)).concat(appendSeparator)
    }else{
      GraphConstants.vertextMaoHao
    }
  }


  /**
   * Get destination string by separator
   *
   * @param  inputValues : need to handle with string values for new graph model
   * @param  separator :  seperator this inputvalues
   * @param  appendSeparator : need to append this seprator to the last of privous inputvalues
   * @return this function has no return values
   */
  def getDestinationValuesByAppendSeparator(inputValues:String, separator:String, appendSeparator:String):String = {
    if(inputValues != "" && inputValues !=null && inputValues.length >0) {
      inputValues.substring(0, inputValues.lastIndexOf(separator)).concat(appendSeparator)
    }else{
      GraphConstants.vertextBlank_1
    }
  }



  /**
   * Get destination map values for the same duduction disease
   *
   * @param  cMapVertexId : need to handle with cMapVertexId
   * @param  cMap : need to handle with map parameters
   * @return this function has no return values
   */
  def getCommonDeductionVertexDisease(cMapVertexId: Long, cMap: Map[Long,String]):String = {
    var totalMapValue : String = ""
    if(cMap.contains(cMapVertexId)){
      val keyM = cMap(cMapVertexId)
      if(keyM != "" && keyM.length >0) {
        totalMapValue = getDestinationValuesByAppendSeparator(keyM, GraphConstants.vertextMaoHao, GraphConstants.vertextBlank)
      }
    }
    totalMapValue
  }


  /**
   * Get final relation values for new graph model
   *
   * @param  vSameRel : same relations between diffient vextex
   * @param  vParentRel : opposite relations between diffierent vextex
   * @return this function has no return values
   */
  def getFinalRelValuesByGraphModel(vSameRel: String, vParentRel:String, separator:String):String = {
    var totalRelValue : String = ""
    if(vSameRel != "" && vSameRel.length >0) {
      totalRelValue = vSameRel + separator + vParentRel
    } else totalRelValue = vParentRel
    totalRelValue
  }

  /**
   * Get final relation values for new graph model
   *
   * @param  vSameRel : same relations between diffient vextex
   * @param  vParentRel : opposite relations between diffierent vextex
   * @return this function has no return values
   */
  def getFinalRelValuesByGraphParas(vSameRel: String, vParentRel:String, separator:String):String = {
    var totalRelValue : String = ""
    if(vSameRel != "" && vSameRel.length >0 && vParentRel !="" && vParentRel.length >0) {
      totalRelValue = vSameRel + separator + vParentRel
    } else if(vSameRel != "" && vSameRel.length >0 && vParentRel =="" && vParentRel.length <= 0){
      totalRelValue = vSameRel
    }else if(vSameRel == "" && vSameRel.length <= 0 && vParentRel !="" && vParentRel.length > 0){
      totalRelValue = vParentRel
    }else{
      totalRelValue
    }
    totalRelValue
  }


  /**
   * Get final relation values for new graph model
   *
   * @param  vSameRel : same relations between diffient vextex
   * @param  vParentRel : opposite relations between diffierent vextex
   * @return this function has no return values
   */
  def getFinalRelValuesByNewGraphModel(vSameRel: String, vParentRel:String, separator:String):String = {
    var totalRelValue : String = ""
    if(vSameRel == "" && vSameRel.length <= 0) {
      totalRelValue
    } else {
      if(vParentRel != "" && vParentRel.length >0){
        totalRelValue = vSameRel + separator +vParentRel
      }else{
        vSameRel
      }

    }
    totalRelValue
  }

  /**
   * Get destination string by separator
   *
   * @param  inputValues : need to handle with string values for new graph model
   * @param  separator :  seperator this inputvalues
   * @param  appendSeparator : need to append this seprator to the last of privous inputvalues
   * @return this function has no return values
   */
  def getDestinationValuesBySeparator(existingValues:String, middleSeparator:String, inputValues:String, separator:String, appendSeparator:String):String = {
    var totalRelValue : String = ""
    if(existingValues != "" && existingValues != null &&  existingValues.length >0){
        if(existingValues != "" && existingValues.length >0){
          val lastValues = existingValues.substring(existingValues.lastIndexOf(middleSeparator)+1,existingValues.length)
          if(lastValues != "" && lastValues.length >0){
            val kallBack = getDestinationValuesBySeparator(inputValues,separator,appendSeparator)
            if(kallBack != "" && kallBack.length >0 ) totalRelValue = GraphConstants.vertextBlankFor + kallBack else kallBack
          }else{
            totalRelValue =  getDestinationValuesBySeparator(inputValues,separator,appendSeparator)
          }
        }
    }else {
      getDestinationValuesBySeparator(inputValues,separator,appendSeparator)
    }
    totalRelValue
  }

  /**
   * 将scan编码，该方法copy自 org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
   *
   * @param scan
   * @return
   * @throws String
   */
  def convertScanToString(scan:Scan):String= {
    val proto = ProtobufUtil.toScan(scan)
    return Base64.encodeBytes(proto.toByteArray())
  }


}
*/
