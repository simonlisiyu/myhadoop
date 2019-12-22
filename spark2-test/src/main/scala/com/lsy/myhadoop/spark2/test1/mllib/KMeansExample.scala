// scalastyle:off println
package com.lsy.myhadoop.spark2.test1.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An example k-means app. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object KMeansExample {

  def main(args: Array[String]) {
    var dataPath = "data/mllib/kmeans_data.txt"
    val conf = new SparkConf().setAppName("KMeansExample")
    if(args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = sc.textFile(dataPath).map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()

    val numExamples = examples.count()

    println(s"numExamples = $numExamples.")

    val k = 2
    val numIterations = 10

    val model = new KMeans()
      .setInitializationMode(KMeans.RANDOM)
      .setK(k)
      .setMaxIterations(numIterations)
      .run(examples)

    val cost = model.computeCost(examples)

    println(s"Total cost = $cost.")

    sc.stop()
  }
}
// scalastyle:on println