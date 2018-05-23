/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import scala.collection.mutable.ArrayBuffer

import com.yahoo.bullet.querying.Querier
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
    queryStream.transformWith(bulletRecordStream, makeTransformFunc(broadcastedConfig) _)
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
        val parallelEnabled = config.get(BulletSparkConfig.FILTER_PARTITION_PARALLEL_MODE_ENABLED).asInstanceOf[Boolean]
        val minQueryThreshold =
          config.get(BulletSparkConfig.FILTER_PARTITION_PARALLEL_MODE_MIN_QUERY_THRESHOLD).asInstanceOf[Int]
        val queryList = broadcastedQueries.value
        if (parallelEnabled && queryList.length >= minQueryThreshold) {
          val filterParallelism = config.get(BulletSparkConfig.FILTER_PARTITION_MODE_PARALLELISM).asInstanceOf[Int]
          val threadPool = Executors.newFixedThreadPool(filterParallelism)
          runInParallel(queryList, records, threadPool, broadcastedConfig).toIterator
        } else {
          queryList.flatMap(
            bulletDataTuple => onData(bulletDataTuple._1, bulletDataTuple._2, records, broadcastedConfig)
          ).toIterator
        }
      })
      broadcastedQueries.unpersist()
      outputRDD
    }
  }

  private def runInParallel(queries: Array[(String, RunningQueryData)], records: List[BulletRecord],
                            threadPool: ExecutorService, broadcastedConfig: Broadcast[BulletSparkConfig]
                           ): Iterable[(String, BulletData)] = {
    val outputs = ArrayBuffer.empty[(String, BulletData)]
    val size = queries.length
    val futures = new Array[Future[Iterable[(String, BulletData)]]](size)
    try {
      for (i <- 0 until size) {
        val bulletDataTuple = queries(i)
        val callable = new Callable[Iterable[(String, BulletData)]]() {
          override def call(): Iterable[(String, BulletData)] = {
            onData(bulletDataTuple._1, bulletDataTuple._2, records, broadcastedConfig)
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

  private def onData(key: String, runningQueryData: RunningQueryData, records: List[BulletRecord],
                     broadCastedConfig: Broadcast[BulletSparkConfig]): Iterable[(String, BulletData)] = {
    val outputs = ArrayBuffer.empty[(String, BulletData)]
    var isDone = false
    val querier = BulletSparkUtils.createBulletQuerier(runningQueryData, Querier.Mode.PARTITION, broadCastedConfig)
    records.foreach(bulletRecord => {
      if (!isDone) {
        querier.consume(bulletRecord)
        if (querier.isDone) {
          outputs += ((key, new FilterResultData(runningQueryData, querier.getData)))
          isDone = true
        } else {
          if (querier.isClosed) {
            outputs += ((key, new FilterResultData(runningQueryData, querier.getData)))
            querier.reset()
          }
          if (querier.isExceedingRateLimit) {
            outputs += ((key, new BulletErrorData(runningQueryData.metadata, querier.getRateLimitError)))
            isDone = true
          }
        }
      }
    })
    // Emit it finally at the end of each batch. But only emit if not done (output already added above) and has data.
    if (!isDone && querier.hasNewData) {
      outputs += ((key, new FilterResultData(runningQueryData, querier.getData)))
    }
    outputs
  }
}
