package com.winhong.datadig.recommender

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by root on 12/6/15.
  */
object ChuanlingALS {
  def main(args: Array[String]) {

    //disable log show
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)



    //设置运行环境
    val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local[5]")
    val sc = new SparkContext(conf)
    println("----------设置运行环境--------")


    //装载用户评分，该评分有评分器生成args1 is user's test data for model tracking
    val myRatings = loadRatings("/root/file/aaa/personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings,1)
    println("----------装载用户评分，该评分有评分器生成--------")
    var m = 1
    myRatings.foreach{ r =>
      println("%2d".format(m) + "\t user = " + r.user +"\t product = " + r.product +"\t rating = " +r.rating)
      m+=1
    }

    //样本数据目录
    val movieLensHomeDir = "/root/file/aaa"

    //装载样本评分数据，其中最后一列时间戳除 10 的余数作为 key ， Rating 为值；
    val ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map{
      line =>
        val fields = line.split("::")
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
    println("----------装载样本评分数据，其中最后一列时间戳除 10 的余数作为 key ， Rating 为值；--------")

    //装载电影目录对照表（电影 ID-> 电影标题）
    val moviesRDD = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map{
      line =>
        val fields = line.split("::")
        (fields(0).toInt, fields(1))
    }.cache()

    val movies = moviesRDD.collect().toMap
    println("----------装载电影目录对照表（电影 ID-> 电影标题）；--------")

    val numRatins = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()

    //val numTotalUsers = ratings.map(_._2.user).distinct().count()


    println("Got " + numRatins +" ratings from " + numUsers +" users on " + numMovies +" movies")

    //将样本评分表以 key 值切分成 3 个部分，分别用于训练 (60% ，并加入用户评分 ), 校验 (20%), and 测试 (20%)
    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6)
      .values
      .union(myRatingsRDD) // 注意 ratings 是 (Int,Rating) ，取 value 即可
      .repartition(numPartitions)
      .cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    //calculator each totally counter's value
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()
    println("----------将样本评分表以 key 值切分成 3 个部分，分别用于训练 (60% ，并加入用户评分 ), 校验 (20%), And 测试 (20%)；--------")
    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    //训练不同参数下的模型，并再校验集中验证，获取最佳参数下的模型
    //------------------------------------validate best model for spark mlib start----------------------------------------
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
    //------------------------------------validate best model for spark mlib end----------------------------------------


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
    println("The best model improves the baseline by " + "%.2f".format(improvement) +"%.")

    //根据用户评分的数据，推荐前十部最感兴趣的电影（注意要剔除用户已经评分的电影）
    val myRatedMovieIds = myRatings.map(x=>x.product).toSet
    //val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map((0, _)))
      .collect()
      .sortBy(-_.rating)
      .take(10)

    println("----------根据用户评分的数据，推荐前十部最感兴趣的电影（注意要剔除用户已经评分的电影）----")

    //------------------------print all top 10 movies film information as below------------------------------
    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies(r.product))
      i += 1
    }


    //结束
    sc.stop()
  }


  //校验预测数据和实际数据之间的方根误差
  def computeRmse(model: MatrixFactorizationModel, data:RDD[Rating], n:Long) :Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))

    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }



  def loadRatings(path : String): Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    val ratings = lines.map { line =>
      val fields = line.split("::")
      //println("fields(0) = " + fields(0).toInt +"\t fields(1) = " + fields(1).toInt +"\t fields(2)="+ fields(2).toDouble)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(r => r.rating > 0.0)

    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }
  }
}
