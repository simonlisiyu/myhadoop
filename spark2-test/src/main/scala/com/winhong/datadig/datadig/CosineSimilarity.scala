/*
package com.winhong.datadig.datadig

import java.io.{File, PrintWriter}

import com.winhong.gzszyy.commons.Tools
import com.winhong.gzszyy.commons.hbase.{HBaseAccessor, HbaseConstantVariables}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import breeze.numerics.{sqrt, pow}
import scala.math._

/**
  * Created by root on 11/18/15.
  */
object CosineSimilarity {
  val tableName = HbaseConstantVariables.getMapHbaseConstantVariables.get("HTABLE_SPARK_DATA_DIG")
  val infoFamily = HbaseConstantVariables.HTABLE_INFO_FAMILY
  val relFamily = HbaseConstantVariables.HTABLE_RELATION_FAMILY

  val keywordMap = scala.collection.mutable.Map(1 -> "name")
  val articleMap = scala.collection.mutable.Map("n a m e" -> "name")
  val tfMap = scala.collection.mutable.Map("a m e n" -> "n a m e")
  val distanceFromCentral = 0.5
  val numOfCsCompare = 3

  def changeToName = (x: Int) => keywordMap.getOrElse(x, "0")

  def changeToSplitWord = (x: String) => {
    if(tfMap.contains(x)) tfMap.get(x)
    else x
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getRDDSecondStrs(nearCsWords:RDD[(String,Double)]):String = {
    var totalString = ""
    for(str <- nearCsWords.collect){
      totalString += str._2 +","
    }
    totalString
  }

  def getRDDFirstStrs(nearCsWords:RDD[(String,Double)]):String = {
    var totalString = ""
    for(str <- nearCsWords.collect){
      totalString += "\""+str._1+"\"" +","
    }
    totalString
  }

  def getTwoLinesCs(line1: Array[String],line2: Array[String]):Double = {
    var upValue = 0.0
    var down1Value = 0.0
    var down2Value = 0.0
    var doubleNm = 0.0

    var forCount = numOfCsCompare

    /*print("line1=")
    line1.foreach(print)
    println("line1.size="+line1.size)
    print("line2=")
    line2.foreach(print)
    println("line2.size="+line2.size)*/
    if(line1.size < forCount || line2.size < forCount){
      println("ssssssssssssssssssssiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiizzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
      if(line1.size < line2.size){
        forCount = line1.size
      }else{
        forCount = line2.size
      }
    }


//    for(i <- 0 to forCount){
    for(i <- 0 to 1){
      upValue += (line1(i).toDouble)*(line2(i).toDouble)
      down1Value += pow(line1(i).toDouble,2)
      down2Value += pow(line2(i).toDouble,2)
    }
    doubleNm = upValue/(sqrt(down1Value*down2Value))


    /*try{
      for(i <- 0 to numOfCsCompare-1){
        upValue += (line1(i).toDouble)*(line2(i).toDouble)
        down1Value += pow(line1(i).toDouble,2)
        down2Value += pow(line2(i).toDouble,2)
      }
      doubleNm = upValue/(sqrt(down1Value*down2Value))
      //    val upValue = ((line1(0).toDouble)*(line2(0).toDouble)) + ((line1(1).toDouble)*(line2(1).toDouble)) + ((line1(2).toDouble)*(line2(2).toDouble))
      //    val down1Value = sqrt(pow(line1(0).toDouble,2) + pow(line1(1).toDouble,2) + pow(line1(2).toDouble,2))
      //    val down2Value = sqrt(pow(line2(0).toDouble,2) + pow(line2(1).toDouble,2) + pow(line2(2).toDouble,2))

    }catch {
      case ex: Exception => {
        line1.foreach(print)
        print("line1=")
        line1.foreach(print)
        println()
        print("line2")
        line2.foreach(print)
        println()
        doubleNm = 0.0
      } // Handle missing file
    }*/
    val res = if(doubleNm < 1.1) doubleNm else 0
    res
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf
//    conf.setAppName("App").setMaster("local[5]")
    val sc = new SparkContext(conf)


    /*
    * test data 1
    * */
//    val hadoopRdd = sc.textFile("hdfs://192.168.190.132:9000/sqoop/input/article"+"fordatadig").map(x => {
//    val hadoopRdd = sc.textFile("hdfs://192.168.190.132:9000/sqoop/input/article"/*+"fordatadig"*/).map(x => {
    val hadoopRdd = sc.textFile(args(0)).map(x => {

      val a = "id="+x.split(",",2)(0)+"id="
      val b = x.split(",",2)(1).replaceAll(", "," ")
      (a+" "+b)
    })
    /*
    * test data 2
    * */
    /*val hadoopRdd = sc.textFile("/sqoop/input/articlefordatadig").map(x => {
      val a = "id="+x.split(",",2)(0)+"id="
      val b = x.split(",",2)(1).replaceAll(", "," ")
      (a+" "+b)
    })*/


    /*
    * real all data
    * */
    /*val hadoopRdd = sc.textFile("/sqoop/input/article").map(x => {
      val a = "id="+x.split(",",2)(0)+"id="
      val b = x.split(",",2)(1).replaceAll(", "," ")
      (a+" "+b)
    })*/

    /*
    * input args data
    * */
    /*val hadoopRdd = sc.textFile(args(0)).map(x => {
      val a = "id="+x.split(",",2)(0)+"id="
      val b = x.split(",",2)(1).replaceAll(", "," ")
      (a+" "+b)
    })*/
    println("--------hadoopRdd--------")
//    hadoopRdd.foreach(println)
    println("--------hadoopRdd--------")

    // Load documents (one per line).
    val documents: RDD[Seq[String]] = hadoopRdd.map(_.split(" ").toSeq)
    documents.foreach(w => {
      for (v <- w) {
        keywordMap += (nonNegativeMod(v.##, 1 << 20) -> v)
      }
    })
    println("--------keywordMap--------")

    val hashingTF = new HashingTF()
    val tf: RDD[linalg.Vector] = hashingTF.transform(documents)
    tf.cache()
    println("--------tftftf--------")

    val tfMapFromRdd = tf.map(line => (line.toSparse.indices -> line.toSparse.values))
    val tfMapRDD = tfMapFromRdd.map(
      line => {
        val kList:Array[String] = line._1.map(changeToName)
        val nameMap = kList.toList.zip(line._2.toList)
        nameMap
      }
    )
    println("--------tfMapFromRdd--------")
//    tfMapFromRdd.foreach(println)

    val idf = new IDF().fit(tf)
    val tfidf: RDD[linalg.Vector] = idf.transform(tf)
    println("--------tfidftfidf--------")

    val mapFromRdd = tfidf.map(line => (line.toSparse.indices -> line.toSparse.values))

    val customerMapRDD = mapFromRdd.map(
      line => {
        val kList:Array[String] = line._1.map(changeToName)
        val nameMap = kList.toList.zip(line._2.toList)
        nameMap
      }
    )
    println("--------customerMapRDD--------")
//    customerMapRDD.foreach(println)

    val joinRdd = tfMapRDD.zip(customerMapRDD)

    val resRdd = joinRdd.map(x => x._1++x._2)
    println("--------resRddresRdd--------")

    var lineWordCss: RDD[String] = null
    var lineTest: Array[String] = null
    for(c <-  resRdd.toArray()){
//      println("--------11111111111--------")
      val wordCs = sc.parallelize(c).reduceByKey((a,b) => sqrt(pow(a,2)+pow(b,2)))    //wordCs = word CosineSimilarity
//      println("--------222222222--------")
      val wordCsScore = wordCs.reduce((a, b) =>{                                 //wordCsScore = word total score
        (a._1+" "+b._1,(a._2+b._2))
      })
//      println("--------333333333--------")
      val scoredWordCs = wordCs.filter(_._2.toString.length > 3)                      //scoredWordCs = scored word CosineSimilarity
//      println("--------4444444444--------")
      val scoredWordCount = scoredWordCs.count()                                                  //wordCount = count the words
//      println("--------5555555555--------")
      val wordCsTotalScore = scoredWordCs.reduce((a, b) =>{                                 //wordCsTotalScore = scored word Total score
        (a._1+","+b._1,(a._2+b._2))
      })
//      println("--------666666666666--------")
      val wordCsTotal = wordCs.reduce((a, b) =>{                                 //wordCsTotal
        (a._1+","+b._1,(a._2+b._2))
      })
//      println("--------777777777--------")
      val lineCs = Tuple1(wordCsTotal._1 -> wordCsTotalScore._2/scoredWordCount)          //lineCs = line CosineSimilarity
//      println("--------888888888--------")
      val nearCsWords = scoredWordCs.map(a => {                                            //nearCsWords = words near line CosineSimilarity
        ((if (a._2-lineCs._1._2 < distanceFromCentral && a._2-lineCs._1._2 > (-distanceFromCentral)) a._2 else 0),a._1)
      }).sortByKey(false).map(b => (b._2,b._1))
//      println("--------9999999999--------")
//      println()
//      print(wordCsScore._1+"--"+getRDDFirstStrs(nearCsWords)+"--"+getRDDSecondStrs(nearCsWords))
//      println()
      val lineWordCssSeq:Seq[String] = Seq[String]().:+(wordCsScore._1+"--"+getRDDFirstStrs(nearCsWords)+"--"+getRDDSecondStrs(nearCsWords))
      lineTest = if(lineTest == null) lineWordCssSeq.toArray else (lineTest ++ lineWordCssSeq)
//      println("--------101010101010--------")
//      val lineWordCs:RDD[String] = sc.parallelize(lineWordCssSeq)    //lineWordCs = words -- nearWords -- nearWordCs
//      println("--------11 11 11 11 11--------")
//      lineWordCss = if(lineWordCss == null) lineWordCs else (lineWordCss ++ lineWordCs)   //lineWordCs = words -- nearWords -- nearWordCs

    }
    lineWordCss = sc.parallelize(lineTest)
    println("--------lineWordCss--------")

//    print("aaaaaaaaaaaaa")
//    println(lineWordCss.partitions.size)
//    lineWordCss.foreach(println)

    var resultRdd: RDD[String] = null
    for(c <-  lineWordCss.toArray()){
      println("cccccccccccc:"+c)
      val thisLine = c.split("--")(2).split(",")
//      println("--------11111111111--------")
      val closedLines = lineWordCss.map(line => {
//        println("line="+line)
        val otherLine = line.split("--")(2).split(",")
        val csValue = getTwoLinesCs(thisLine,otherLine)
        (csValue, line.split("--")(0))
      })
//      println("--------222222222--------")
      val closedLinesSorted = closedLines/*.filter(_._1 != 0)*/.sortByKey(false).map(kv => kv._2).collect
//      println("--------33333333--------")
      val closedLinesSortedReduce = closedLinesSorted.reduce((a,b) => a+"--"+b)
//      println("--------4444444444--------")
      val resultSeq:Seq[String] = Seq[String]().:+(c.split("--")(0)+"--"+c.split("--")(1)+"--"+closedLinesSortedReduce)
//      println("--------555555555--------")
      val resRdd:RDD[String] = sc.parallelize(resultSeq)    //resRdd = thisLine -- "linekeyword1","linekeyword2",... -- closedLine1 -- closedLine2 ...
//      println("--------666666666--------")
      resultRdd = if(resultRdd == null) resRdd else (resultRdd ++ resRdd)   //resultRdd = thisLine -- "linekeyword1","linekeyword2",... -- closedLine1 -- closedLine2 ...
//      println("--------7777777777--------")
    }

    println("ooooo----resultRdd----oooooo")
//    resultRdd.foreach(println)

    val retRdd = resultRdd.map(
      line => {
        val lineSplit = line.split("--")
        val articleId = lineSplit(0).split("id=")(1)
        val keyWords = lineSplit(1)
        var closedLine = ""
        for(i <- 2 to lineSplit.length-1){
          closedLine += lineSplit(i).split("id=")(1)+"--"
        }
        (articleId,keyWords,closedLine)
      }
    )

    println("retRddretRddretRdd")
    retRdd.foreach(println)

    println("--------------")

    if(HBaseAccessor.exists(tableName)){
      HBaseAccessor.deleteTable(tableName)
      HBaseAccessor.create(tableName,infoFamily,relFamily)
    }else{
      HBaseAccessor.create(tableName,infoFamily,relFamily)
    }

    retRdd.map(x => {
      val articleId = x._1
      val keywords = x._2.split(",")
      val closedIds = x._3
      var columnInfo = Array("articleNo")
      var valueInfo = Array(articleId)
      var i = 0
      for(keyword <- keywords){
        i = i+1
        columnInfo = columnInfo.:+("keyword_"+i)
        valueInfo = valueInfo.:+(keyword)
      }
      i = 0
      val columnRel = Array("closedNos")
      val valueRel = Array(closedIds)

      val rowId = Tools.randomMd5String(articleId)
      HBaseAccessor.addData(rowId,tableName,infoFamily,relFamily,columnInfo,valueInfo,columnRel,valueRel)
      columnInfo.foreach(println)
      println("--------------")
      valueInfo.foreach(println)
      println("--------------")
      columnRel.foreach(println)
      println("--------------")
      valueRel.foreach(println)
      println("--------------")

      x
    }).collect()

    sc.stop()
  }



}
*/
