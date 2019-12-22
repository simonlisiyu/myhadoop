package com.lsy.myhadoop.spark2.compute

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SparkCacheCheckPointApi {
  // Hadoop MapReduce 在执行 job 的时候，不停地做持久化，每个 task 运行结束做一次，每个 job 运行结束做一次（写到 HDFS）。
  // 在 task 运行过程中也不停地在内存和磁盘间 swap 来 swap 去。 可是讽刺的是，Hadoop 中的 task 太傻，中途出错需要完全重新运行，
  // 比如 shuffle 了一半的数据存放到了磁盘，下次重新运行时仍然要重新 shuffle。Spark 好的一点在于尽量不去持久化，
  // 所以使用 pipeline，cache 等机制。用户如果感觉 job 可能会出错可以手动去 checkpoint 一些 critical 的 RDD，job 如果出错，
  // 下次运行时直接从 checkpoint 中读取数据。唯一不足的是，checkpoint 需要两次运行 job。

  // cache 与 checkpoint 的区别？
  // 关于这个问题，Tathagata Das 有一段回答: There is a significant difference between cache and checkpoint.
  // Cache materializes the RDD and keeps it in memory and/or disk（其实只有 memory）.
  // But the lineage（也就是 computing chain） of RDD (that is, seq of operations that generated the RDD) will be remembered,
  // so that if there are node failures and parts of the cached RDDs are lost, they can be regenerated.
  // However, checkpoint saves the RDD to an HDFS file and actually forgets the lineage completely.
  // This is allows long lineages to be truncated and the data to be saved reliably in
  // HDFS (which is naturally fault tolerant by replication).

  // 深入一点讨论，rdd.persist(StorageLevel.DISK_ONLY) 与 checkpoint 也有区别。
  // 前者虽然可以将 RDD 的 partition 持久化到磁盘，但该 partition 由 blockManager 管理。
  // 一旦 driver program 执行结束，也就是 executor 所在进程 CoarseGrainedExecutorBackend stop，blockManager 也会 stop，
  // 被 cache 到磁盘上的 RDD 也会被清空（整个 blockManager 使用的 local 文件夹被删除）。
  // 而 checkpoint 将 RDD 持久化到 HDFS 或本地文件夹，如果不被手动 remove 掉（话说怎么 remove checkpoint 过的 RDD？），
  // 是一直存在的，也就是说可以被下一个 driver program 使用，而 cached RDD 不能被其他 dirver program 使用。

  // init spark session
  val spark = SparkSession.builder().appName(s"CityOrderGpsData").enableHiveSupport().getOrCreate()
  val sc = spark.sparkContext

  sc.setCheckpointDir("/Users/data/checkpoint")
  val data = Array[(Int, Char)]((1, 'a'), (2, 'b'),
    (3, 'c'), (4, 'd'),
    (5, 'e'), (3, 'f'),
    (2, 'g'), (1, 'h')
  )
  val pairs = sc.parallelize(data, 3)

  pairs.cache()
//  pairs.persist(StorageLevel.MEMORY_AND_DISK_2)
//  pairs.persist(StorageLevel.OFF_HEAP)

  pairs.checkpoint
  pairs.count

  val result = pairs.groupByKey(2)
  println(result.toDebugString)

}
