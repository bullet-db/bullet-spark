/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.pubsub.Metadata.Signal
import com.yahoo.bullet.pubsub.PubSubMessage
import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.data.{BulletData, BulletErrorData, BulletSignalData, RunningQueryData}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object QueryDataUnioning {
  /**
   * Generates and returns the input query stream and the BulletRecord stream.
   *
   * @param ssc               The Spark streaming context
   * @param broadcastedConfig The broadcasted [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] configuration
   *                          that has been validated.
   * @return A tuple with the input query stream and the BulletRecord stream.
   */
  def receive(ssc: StreamingContext, broadcastedConfig: Broadcast[BulletSparkConfig]
             ): (DStream[(String, BulletData)], DStream[BulletRecord]) = {
    val config = broadcastedConfig.value
    val queryCoalescePartitions = config.get(BulletSparkConfig.QUERY_COALESCE_PARTITIONS).asInstanceOf[Int]
    // Input query stream.
    val queryStream = ssc.receiverStream(new QueryReceiver(config))
      .transform(_.coalesce(queryCoalescePartitions, shuffle = false))
    // Only get valid queries.
    val validQueries = getAllValidQueries(ssc, queryStream, broadcastedConfig)
    (validQueries, getBulletRecordStream(ssc, config))
  }

  private[spark] def getAllValidQueries(ssc: StreamingContext, queryStream: DStream[PubSubMessage],
                                        broadcastedConfig: Broadcast[BulletSparkConfig]
                                       ): DStream[(String, BulletData)] = {
    // Create (id, BulletData) pairs out of the queries.
    val metrics = BulletSparkMetrics.getInstance(ssc, broadcastedConfig)
    val queryPairStream = queryStream.map(m => {
      val output = (m.getId, BulletSparkUtils.createBulletData(m, broadcastedConfig))
      if (!output._2.isInstanceOf[BulletSignalData]) {
        BulletSparkMetrics.newQueryReceived(metrics)
      }
      output
    })

    // Generate query stream including valid queries.
    val config = broadcastedConfig.value
    val duration = config.get(BulletSparkConfig.BATCH_DURATION_MS).asInstanceOf[Int]
    val durationMultiplier = config.get(BulletSparkConfig.QUERY_UNION_CHECKPOINT_DURATION_MULTIPLIER).asInstanceOf[Int]
    queryPairStream.updateStateByKey(updateFunc).cache()
      .checkpoint(Durations.milliseconds(durationMultiplier * duration))
  }

  private def updateFunc(queryList: Seq[BulletData], state: Option[BulletData]): Option[BulletData] = {
    state match {
      case Some(bulletData) =>
        // If existing state is a signal or an error, remove it.
        if (bulletData.isInstanceOf[BulletSignalData] || bulletData.isInstanceOf[BulletErrorData]) {
          None
        } else {
          // Find all the signals.
          val list = queryList.filter(_.isInstanceOf[BulletSignalData])
          if (list.isEmpty) {
            // No signals so received RunningQueryData. If it timed out, change it to a COMPLETE else save it.
            if (bulletData.asInstanceOf[RunningQueryData].runningQuery.isTimedOut) {
              Option(new BulletSignalData(bulletData.metadata, Signal.COMPLETE))
            } else {
              Option(bulletData)
            }
          } else {
            // Received KILL or COMPLETE signal queries. Pick any of them as the state.
            Option(list.head)
          }
        }
      case None => Option(queryList.head)
    }
  }

  private def getBulletRecordStream(ssc: StreamingContext, config: BulletSparkConfig): DStream[BulletRecord] = {
    // Setup and combine the BulletRecord input streams.
    val bulletRecordParallelism = config.get(BulletSparkConfig.DATA_PRODUCER_PARALLELISM).asInstanceOf[Int]
    val bulletRecordStreams =
      (1 to bulletRecordParallelism).map(_ => DataProducer.getProducer(config).getBulletRecordStream(ssc, config))
    ssc.union(bulletRecordStreams)
  }
}
