/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.util.Optional

// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on

import com.yahoo.bullet.pubsub.PubSubMessage
import com.yahoo.bullet.querying.{Querier, RunningQuery}
import com.yahoo.bullet.spark.data.{
  BulletData, BulletErrorData, BulletSignalData, FilterResultData, QuerierData, RunningQueryData
}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkLogger}
import org.apache.spark.{BulletSparkMetricsSource, SparkConf}
import org.apache.spark.streaming.{Durations, StreamingContext}

object BulletSparkStreamingBaseJob {
  val CLASSES: Array[Class[_]] = Array(
    classOf[RunningQuery], classOf[Querier], classOf[PubSubMessage], classOf[BulletData], classOf[BulletErrorData],
    classOf[QuerierData], classOf[RunningQueryData], classOf[BulletSignalData], classOf[FilterResultData])
}

class BulletSparkStreamingBaseJob extends BulletSparkLogger {
  def getOrCreateContext(config: BulletSparkConfig): StreamingContext = {
    val recoverFromCheckpoint = config.get(BulletSparkConfig.RECOVER_FROM_CHECKPOINT_ENABLE).asInstanceOf[Boolean]
    if (recoverFromCheckpoint) {
      val checkpointDir = config.get(BulletSparkConfig.CHECKPOINT_DIR).asInstanceOf[String]
      StreamingContext.getOrCreate(checkpointDir, () => createContext(config))
    } else {
      createContext(config)
    }
  }

  def createContext(config: BulletSparkConfig): StreamingContext = {
    val ssc = initStreamingContext(config)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)

    // Get input streams.
    val (queryStream, bulletRecordStream) = QueryDataUnioning.receive(ssc, broadcastedConfig)
    queryStream.cache()

    // Filter bullet record stream by query stream.
    val runningQueryStream = queryStream.filter(_._2.isInstanceOf[RunningQueryData]).map(tuple => (tuple._1, tuple._2.asInstanceOf[RunningQueryData]))
    val filteredQueriesStreams = FilterStreaming.filter(runningQueryStream, bulletRecordStream, broadcastedConfig)

    // Join the filter query stream.
    val resultDStream = JoinStreaming.join(ssc, filteredQueriesStreams.union(queryStream), broadcastedConfig)

    // Publish result.
    ResultEmitter.emit(resultDStream, ResultPublisher.getInstance(ssc, broadcastedConfig))

    ssc
  }

  private def initStreamingContext(config: BulletSparkConfig): StreamingContext = {
    // Construct the Spark Streaming Context.
    val appName = config.get(BulletSparkConfig.APP_NAME).asInstanceOf[String]
    val conf = new SparkConf().registerKryoClasses(BulletSparkStreamingBaseJob.CLASSES).setAppName(appName)

    // Set all Spark setting as is.
    val sparkConfig = config.getAllWithPrefix(Optional.empty(), BulletSparkConfig.SPARK_STREAMING_CONFIG_PREFIX, false)
    sparkConfig.asScala.foreach(c => conf.set(c._1, c._2.asInstanceOf[String]))

    val duration = config.get(BulletSparkConfig.BATCH_DURATION_MS).asInstanceOf[Int]
    val ssc = new StreamingContext(conf, Durations.milliseconds(duration))

    // Set the checkpoint directory.
    val checkpointDir = config.get(BulletSparkConfig.CHECKPOINT_DIR).asInstanceOf[String]
    ssc.checkpoint(checkpointDir)

    // Register metrics.
    BulletSparkMetricsSource.register(ssc, config)

    ssc
  }
}
