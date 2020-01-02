/*
package com.winhong.datadig.recommender

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.winhong.gzszyy.commons.redis.RedisReader
import com.winhong.gzszyy.commons.Tools
import com.winhong.gzszyy.core.search.recommendation.service.impl.RecommendServiceImpl
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 12/6/15.
  */
object HistoryRecommendForEveryUser {
  val keywordMap = scala.collection.mutable.Map("word" -> "00")
  val idWordMap = scala.collection.mutable.Map("00" -> "word")
//  val recommendForUsersMap = scala.collection.mutable.Map(0 -> "word")
  val recommendForUsersMap = new util.HashMap[Integer,String]()

  def getNowDate():String={
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateStr = dateFormat.format( now )
    dateStr
  }

  def getBefore30Date():String= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -30)
    val dateStr = dateFormat.format(cal.getTime())
    dateStr
  }

  //校验预测数据和实际数据之间的方根误差
  def computeRmse(model: MatrixFactorizationModel, data:RDD[Rating], n:Long) :Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))

    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf
    //conf.setAppName("MovieLensALS").setMaster("local[5]")
    conf.setAppName("UserSearchALS").setMaster("local[5]")

    val sc = new SparkContext(conf)

    println("----------装载keywords表；--------")
    val keywordSet = RedisReader.getAllKeyWord.toArray
    val keywordsRDD = sc.parallelize(keywordSet).mapPartitionsWithIndex{
      (x,iter) => {
        val result = List[String]()
        var it = 0
        while(iter.hasNext){
          it += 1
          val word = iter.next().toString.split("-")(0)
          keywordMap.put(word,it+""+x)
          idWordMap.put(it+""+x,word)
          word
        }
        result.::(x + "|" + it).iterator
      }
    }.collect()

    println("keywordMap => " + keywordMap.size)
//    keywordMap.foreach(println)
    println("----------装载keywords表；--------")



    println("----------装载all user search数据，其中最后一列时间戳除 10 的余数作为 key ， Rating 为值；--------")
    val recoSer = new RecommendServiceImpl();
    val userLogList = recoSer.getUserLogsByDate(getBefore30Date(),getNowDate()).toArray()
    val reduceSample = sc.parallelize(userLogList).map(x => {
      val splits = x.toString.split("\t")
      println("0: "+splits(0)+",1: "+splits(1)+",2: "+splits(2))
      (splits(0)+"-"+splits(1),splits(2).toInt)
    }).reduceByKey(_ + _)
//    reduceSample.foreach(println)

    val textSample = reduceSample.map(x => {
      val splits = x._1.split("-")
      val wordId = keywordMap.getOrElse(splits(1),"0")
      (Math.random()*10000%10 , Rating(splits(0).toInt, wordId.toInt, x._2.toDouble))
    })
//    textSample.foreach(println)

    val numRatings = textSample.count()
    val userIdRDD = textSample.map(_._2.user).distinct().toArray()
    val numUsers = textSample.map(_._2.user).distinct().count()
    val numWords = textSample.map(_._2.product).distinct().count()

//    textSample.map(_._2.product).distinct().foreach(println)

    println("Got " + numRatings +" ratings from " + numUsers +" users on " + numWords +" words")
    println("----------装载all user search数据，其中最后一列时间戳除 10 的余数作为 key ， Rating 为值；--------")



    println("----------将样本评分表以 key 值切分成 3 个部分，分别用于训练 (60% ，并加入用户评分 ), 校验 (20%), And 测试 (20%)；--------")
    //将样本评分表以 key 值切分成 3 个部分，分别用于训练 (60% ，并加入用户评分 ), 校验 (20%), and 测试 (20%)
    val numPartitions = 4
    val training = textSample.filter(x => x._1 < 6)
      .values
//      .union(myRatingsRDD) // 注意 ratings 是 (Int,Rating) ，取 value 即可
      .repartition(numPartitions)
      .cache()
    val validation = textSample.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()
    val test = textSample.filter(x => x._1 >= 8).values.cache()


    //calculator each totally counter's value
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)
    println("----------将样本评分表以 key 值切分成 3 个部分，分别用于训练 (60% ，并加入用户评分 ), 校验 (20%), And 测试 (20%)；--------")




    println("----------训练不同参数下的模型，并再校验集中验证，获取最佳参数下的模型----")
    //训练不同参数下的模型，并再校验集中验证，获取最佳参数下的模型
    //------------------------------------validate best model for spark mlib start----------------------------------------
    /*val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    */
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(3)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) { //for 8 loop
    val model = ALS.train(training, rank, numIter, lambda)

      val validationRmse = computeRmse(model, validation, numValidation)
      //print following information as below
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }

    }
    println("----------训练不同参数下的模型，并再校验集中验证，获取最佳参数下的模型----")


    println("----------用最佳模型预测测试集的评分，计算和实际评分之间的均方根误差----")
    //，计算和实际评分之间的均方根用最佳模型预测测试集的评分误差
    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank +" and lamdba = " + bestLambda
      +", and numIter = " + bestNumIter +", and its RMSE on the test set is " + testRmse +".")
    println("----------用最佳模型预测测试集的评分，计算和实际评分之间的均方根误差----")
    //-------------------performance consideration as below----------------
    //create a native baseline and compare it with the best model
    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("improvement=>"+improvement)
    println("The best model improves the baseline by " + "%.2f".format(improvement) +"%.")



    println("----------根据用户评分的数据，推荐前十部最感兴趣的电影（注意要剔除用户已经评分的电影）----")
    //根据用户评分的数据，推荐前十部最感兴趣的电影（注意要剔除用户已经评分的电影）

    userIdRDD.foreach { x =>
      println("x: "+x)

      try{
        val recommendations = bestModel.get.recommendProducts(x, 10)
        //      recommendations.foreach(println)
        //------------------------print all top 10 movies film information as below------------------------------
        var i = 1
        println("Keywords recommended for you:")

        var keywordStr = ""

        recommendations.foreach { r =>
          println("%2d".format(i) + ": " + idWordMap.getOrElse(r.product.toString,"0"))
          keywordStr += idWordMap.getOrElse(r.product.toString,"-")+"-"
          i += 1
        }

        println("keywordStr: "+keywordStr)
        recoSer.saveRecommendationToRedis(x,keywordStr)
//        recommendForUsersMap.put(x,keywordStr)
      }catch {
        case ex: NoSuchElementException => // Handle next on empty iterator
      }
    }


    println("-----------------------------------------------------------------------------------")
    //candidates.map((0, _)).foreach(println)
    println("-----------------------------------------------------------------------------------")

    println("----------根据用户评分的数据，推荐前十部最感兴趣的电影（注意要剔除用户已经评分的电影）----")

//    recoSer.saveRecommendationToRedis(recommendForUsersMap)


    //结束
    sc.stop()
  }

}
*/
