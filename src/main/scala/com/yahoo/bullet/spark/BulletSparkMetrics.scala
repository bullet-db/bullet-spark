/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.spark.BulletSparkMetrics.{QUERIES_DONE_METRIC, QUERIES_RECEIVED_METRIC, QUERIES_RUNNING_METRIC}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkLogger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.LongAccumulator

object BulletSparkMetrics {
  val QUERIES_RECEIVED_METRIC = "queriesReceived"
  val QUERIES_RUNNING_METRIC = "queriesRunning"
  val QUERIES_DONE_METRIC = "queriesDone"

  @volatile private var instance: BulletSparkMetrics = _

  def getInstance(ssc: StreamingContext, broadcastedConfig: Broadcast[BulletSparkConfig]): BulletSparkMetrics = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val metricsEnabled = broadcastedConfig.value.get(BulletSparkConfig.METRICS_ENABLED).asInstanceOf[Boolean]
          instance = new BulletSparkMetrics(ssc, metricsEnabled)
        }
      }
    }
    instance
  }

  /**
   * Update the metrics when receiving a query.
   *
   * @param metrics The [[com.yahoo.bullet.spark.BulletSparkMetrics]] instance.
   */
  def newQueryReceived(metrics: BulletSparkMetrics): Unit = {
    metrics.increase(BulletSparkMetrics.QUERIES_RECEIVED_METRIC)
    metrics.increase(BulletSparkMetrics.QUERIES_RUNNING_METRIC)
  }

  /**
   * Update the metrics when a query is done.
   *
   * @param metrics The [[com.yahoo.bullet.spark.BulletSparkMetrics]] instance.
   */
  def queryFinished(metrics: BulletSparkMetrics): Unit = {
    metrics.increase(BulletSparkMetrics.QUERIES_DONE_METRIC)
    metrics.decrease(BulletSparkMetrics.QUERIES_RUNNING_METRIC)
  }
}

/**
 * @constructor Create a instance to hold all metric accumulators.
 * @param ssc            The Spark streaming context.
 * @param metricsEnabled The Boolean indicating whether the metrics are enabled or not.
 */
class BulletSparkMetrics(ssc: StreamingContext, val metricsEnabled: Boolean)
  extends BulletSparkLogger with Serializable {
  val metricsMap: Map[String, LongAccumulator] =
    if (metricsEnabled) {
      Map(
        QUERIES_RECEIVED_METRIC -> createAccumulator(ssc, QUERIES_RECEIVED_METRIC),
        QUERIES_RUNNING_METRIC -> createAccumulator(ssc, QUERIES_RUNNING_METRIC),
        QUERIES_DONE_METRIC -> createAccumulator(ssc, QUERIES_DONE_METRIC)
      )
    } else {
      null
    }

  /**
   * Increase the value of the given metric.
   *
   * @param name The name string of the metric.
   */
  def increase(name: String): Unit = {
    if (metricsEnabled) {
      metricsMap.get(name) match {
        case Some(accumulator) => accumulator.add(1L)
        case None => logger.error("Cannot find metric ", name)
      }
    }
  }

  /**
   * Decrease the value of the given metric.
   *
   * @param name The name string of the metric.
   */
  def decrease(name: String): Unit = {
    if (metricsEnabled) {
      metricsMap.get(name) match {
        case Some(accumulator) => accumulator.add(-1L)
        case None => logger.error("Cannot find metric ", name)
      }
    }
  }

  private def createAccumulator(ssc: StreamingContext, name: String): LongAccumulator = {
    ssc.sparkContext.longAccumulator(name)
  }
}
