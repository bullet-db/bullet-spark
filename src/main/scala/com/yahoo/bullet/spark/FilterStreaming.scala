/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.util.concurrent.{Callable, Executors, Future}

import scala.collection.mutable.ArrayBuffer

// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on

import com.yahoo.bullet.querying.{Querier, QueryManager}
import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.data.{BulletData, BulletErrorData, FilterResultData, RunningQueryData}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object FilterStreaming {
  /**
   * Filter and apply partial aggregations for each query in the stream using the BulletRecord stream .
   *
   * This method takes the query stream and BulletRecord stream, and outputs [[com.yahoo.bullet.spark.data.BulletData]]
   * stream (They can be [[com.yahoo.bullet.spark.data.BulletErrorData]] or
   * [[com.yahoo.bullet.spark.data.FilterResultData]])) to the JoinStreaming phase.
   * In this method, the query stream is collected and broadcast to all executors. In each partition,
   * queries consume the Bullet records and generate the output [[com.yahoo.bullet.spark.data.BulletData]] accordingly.
   *
   * @param queryStream        The input query stream.
   * @param bulletRecordStream The input BulletRecord stream.
   * @param broadcastedConfig  The broadcasted [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] configuration
   *                           that has been validated.
   * @return A [[com.yahoo.bullet.spark.data.BulletData]] stream which contains the intermediate results for queries.
   */
  def filter(queryStream: DStream[(String, RunningQueryData)], bulletRecordStream: DStream[BulletRecord],
             broadcastedConfig: Broadcast[BulletSparkConfig]): DStream[(String, BulletData)] = {
    queryStream.transformWith(bulletRecordStream, makeTransformFunc(broadcastedConfig) _).cache()
  }

  private def makeTransformFunc(broadcastedConfig: Broadcast[BulletSparkConfig])
                               (validQueriesRDD: RDD[(String, RunningQueryData)],
                                bulletRecordRDD: RDD[BulletRecord]): RDD[(String, BulletData)] = {
    // Broadcast valid queries and join with the BulletRecord stream to generate partial results which have already
    // consumed the BulletRecord instances successfully.
    val queries = validQueriesRDD.collect()
    if (queries.isEmpty) {
      validQueriesRDD.context.emptyRDD[(String, BulletData)]
    } else {
      // Broadcast the queries to all executors using a context (using the context from the validQueriesRDD).
      val broadcastedQueries = validQueriesRDD.context.broadcast(queries)
      val outputRDD = bulletRecordRDD.mapPartitions(bulletRecordIterator => {
        val records = bulletRecordIterator.toList
        val config = broadcastedConfig.value
        val queryList = broadcastedQueries.value
        val parallelEnabled = config.get(BulletSparkConfig.FILTER_PARTITION_PARALLEL_MODE_ENABLED).asInstanceOf[Boolean]
        val minQueryThreshold =
          config.get(BulletSparkConfig.FILTER_PARTITION_PARALLEL_MODE_MIN_QUERY_THRESHOLD).asInstanceOf[Int]
        val filterParallelism = config.get(BulletSparkConfig.FILTER_PARTITION_MODE_PARALLELISM).asInstanceOf[Int]
        if (parallelEnabled && queryList.length >= minQueryThreshold && queryList.length >= filterParallelism) {
          runInParallel(queryList, records, broadcastedConfig, filterParallelism).toIterator
        } else {
          process(queryList, records, broadcastedConfig).toIterator
        }
      })
      broadcastedQueries.unpersist()
      outputRDD
    }
  }

  private def runInParallel(queries: Array[(String, RunningQueryData)], records: List[BulletRecord],
                            broadcastedConfig: Broadcast[BulletSparkConfig], filterParallelism: Int
                           ): Iterable[(String, BulletData)] = {
    val outputs = ArrayBuffer.empty[(String, BulletData)]
    val threadPool = Executors.newFixedThreadPool(filterParallelism)
    val totalSize = queries.length
    val subSize = totalSize / filterParallelism
    val futures = new Array[Future[Iterable[(String, BulletData)]]](filterParallelism)

    var start = 0
    try {
      for (i <- 0 until filterParallelism) {
        val end = if (i == filterParallelism - 1) totalSize else start + subSize
        val subQueries = queries.slice(start, end)
        start = end
        val callable = new Callable[Iterable[(String, BulletData)]]() {
          override def call(): Iterable[(String, BulletData)] = {
            process(subQueries, records, broadcastedConfig)
          }
        }
        futures(i) = threadPool.submit(callable)
      }
    } finally {
      threadPool.shutdown()
    }

    futures.foreach(outputs ++= _.get())
    outputs
  }

  private def process(queryList: Array[(String, RunningQueryData)], records: List[BulletRecord],
                      broadcastedConfig: Broadcast[BulletSparkConfig]): Iterable[(String, BulletData)] = {
    val queryManager = new QueryManager(broadcastedConfig.value)
    queryList.foreach(entry => {
      val querier = BulletSparkUtils.createBulletQuerier(entry._2, Querier.Mode.PARTITION, broadcastedConfig)
      queryManager.addQuery(entry._1, querier)
    })
    val outputs = ArrayBuffer.empty[(String, BulletData)]
    val queryMap = queryList.toMap
    records.foreach(record => onData(queryMap, queryManager, record, outputs))
    emitUnDoneQuries(queryMap, queryManager, outputs)
    outputs
  }

  private def onData(queryMap: Map[String, RunningQueryData], queryManager: QueryManager, record: BulletRecord,
                     outputs: ArrayBuffer[(String, BulletData)]): Unit = {
    val queryCategorizer = queryManager.categorize(record)

    queryCategorizer.getDone.asScala.foreach(entry => {
      val querier = entry._2
      val key = entry._1
      outputs += ((key, new FilterResultData(queryMap(key), querier.getData)))
      queryManager.removeAndGetQuery(key)
    })

    queryCategorizer.getRateLimited.asScala.foreach(entry => {
      val querier = entry._2
      val key = entry._1
      outputs += ((key, new BulletErrorData(queryMap(key).metadata, querier.getRateLimitError)))
      queryManager.removeAndGetQuery(key)
    })

    queryCategorizer.getClosed.asScala.foreach(entry => {
      val querier = entry._2
      val key = entry._1
      outputs += ((key, new FilterResultData(queryMap(key), querier.getData)))
      querier.reset()
    })
  }

  private def emitUnDoneQuries(queryMap: Map[String, RunningQueryData], queryManager: QueryManager,
                               outputs: ArrayBuffer[(String, BulletData)]): Unit = {
    val queries = queryManager.getQueries
    queries.asScala.foreach(entry => {
      val querier = entry._2
      val key = entry._1
      if (querier.hasNewData) {
        outputs += ((key, new FilterResultData(queryMap(key), querier.getData)))
      }
    })
  }
}
