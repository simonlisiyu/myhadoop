package com.lsy.myhadoop.spark2.compute

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object SparkComputeApi {
  /**
    * Spark 算子说明
    * http://lxw1234.com/archives/2015/07/363.htm
    */

  def main(args: Array[String]): Unit = {
    // init spark session
    val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val pets = sc.parallelize(List(("cat", 1), ("dog", 1), ("cat", 2)))
    // 所有key/value RDD操作符均包含一个可选参数，表示reduce task并行度
    // 用户也可以通过修改spark.default.parallelism设置默认并行度,默认并行度为最初的RDD partition数目
    pets.reduceByKey(_ + _, 5) // => {(cat, 3), (dog, 1)}  reduceByKey自动在map端进行本地combine
    pets.groupByKey() // => {(cat, Seq(1, 2)), (dog, Seq(1)}
    pets.sortByKey() // => {(cat, 1), (cat, 2), (dog, 1)}

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKey(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { case (name, (num, socre)) => (name, socre / num) }.collect


    val visits = sc.parallelize(List(("index.html", "1.2.3.4"), ("about.html", "2.3.4.5"), ("index.html", "3.4.5.6")))
    val pageNames = sc.parallelize(List(("index.html", "Home"), ("about.html", "About")))

    visits.join(pageNames).collect  //Array[(String, (String, String))] = Array((about.html,(2.3.4.5,About)), (index.html,(1.2.3.4,Home)), (index.html,(3.4.5.6,Home)))
    visits.cogroup(pageNames).collect   //Array[(String, (Iterable[String], Iterable[String]))] = Array((about.html,(CompactBuffer(2.3.4.5),CompactBuffer(About))), (index.html,(CompactBuffer(1.2.3.4, 3.4.5.6),CompactBuffer(Home))))
    visits.union(pageNames).collect   //Array[(String, String)] = Array((index.html,1.2.3.4), (about.html,2.3.4.5), (index.html,3.4.5.6), (index.html,Home), (about.html,About))
    visits.cartesian(pageNames).collect   //Array[((String, String), (String, String))] = Array(((index.html,1.2.3.4),(index.html,Home)), ((index.html,1.2.3.4),(about.html,About)), ((about.html,2.3.4.5),(index.html,Home)), ((about.html,2.3.4.5),(about.html,About)), ((index.html,3.4.5.6),(index.html,Home)), ((index.html,3.4.5.6),(about.html,About)))

    val df1 = spark.read.json("/home/data1.json")
    val df2 = spark.read.json("/home/data2.json")
    val mergedDataFrame2 = df1.filter("movieID = 2116").
      join(df2, df1("userID") === df2("userID"), "inner").select("gender", "age").
      groupBy("gender", "age").
      count
    // SELECT statement FROM statement
    //  [JOIN | INNER JOIN | LEFT JOIN | LEFT SEMI JOIN | LEFT OUTER JOIN | RIGHT JOIN | RIGHT OUTER JOIN | FULL JOIN | FULL OUTER JOIN]
    //  ON join condition

    df1.createOrReplaceTempView("users")    //只在session范围内有效，Session结束表自动删除
    val groupedUsers = spark.sql("select gender, age, count(*) as n from users group by gender, age")
    groupedUsers.show()

    df1.createGlobalTempView("people")    //全局范围内的临时表
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

    df1.write.mode("overwrite").saveAsTable("database.tableName") //将DataFrame或Dataset持久化到Hive中(需把hive配置放到环境中)
  }


  /**
    * flat map + 隐式转换
    * @param spark
    * @param dataFrame
    * @return
    */
  def flatMapWithImplicit(spark: SparkSession, dataFrame: DataFrame) ={
    import spark.implicits._
    implicit val matchError = org.apache.spark.sql.Encoders.tuple( Encoders.STRING,
      Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.INT)
    val stepFourDF = dataFrame.flatMap(item => {
      val array = Array("1", "2", "3")
      array.map(cell_id => (item.getAs[String]("id"),
        item.getAs[String]("name"),
        item.getAs[String]("a_id")+","+item.getAs[String]("time")+","+item.getAs[String]("b_id"),
        cell_id,
        array.length
      )
      )
    })
    stepFourDF.toDF("id", "name", "id_time", "cell_id", "length")
  }
}
