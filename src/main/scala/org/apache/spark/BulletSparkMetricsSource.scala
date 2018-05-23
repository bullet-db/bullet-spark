/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package org.apache.spark

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.yahoo.bullet.spark.utils.BulletSparkConfig
import org.apache.spark.metrics.source.Source
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.LongAccumulator
import com.yahoo.bullet.spark.BulletSparkMetrics

object BulletSparkMetricsSource {
  /**
   * Register metric accumulators to the Spark metrics source. They can be accessed via Spark REST API (/metrics/json).
   *
   * @param ssc    The Spark streaming context
   * @param config The [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] configuration.
   */
  def register(ssc: StreamingContext, config: BulletSparkConfig): Unit = {
    val metricsEnabled = config.get(BulletSparkConfig.METRICS_ENABLED).asInstanceOf[Boolean]
    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val metrics = BulletSparkMetrics.getInstance(ssc, broadcastedConfig)
    register(ssc, metrics.metricsMap, metricsEnabled)
  }

  private[spark] def register(ssc: StreamingContext, accumulators: Map[String, LongAccumulator],
                              enabled: Boolean): Unit = {
    if (enabled) {
      val source = new BulletSparkMetricsSource
      accumulators.foreach {
        case (name, accumulator) => source.register(name, accumulator)
      }
      ssc.sparkContext.env.metricsSystem.registerSource(source)
    }
  }
}

class BulletSparkMetricsSource extends Source {
  private val registry = new MetricRegistry

  private def register(name: String, accumulator: LongAccumulator): Unit = {
    registry.register(MetricRegistry.name(name), new Gauge[Long] {
      override def getValue: Long = accumulator.value
    })
  }

  override def sourceName: String = "BulletSparkMetricsSource"

  override def metricRegistry: MetricRegistry = registry
}
