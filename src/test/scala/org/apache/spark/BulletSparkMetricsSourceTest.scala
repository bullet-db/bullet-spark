/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package org.apache.spark

import com.yahoo.bullet.spark.BulletSparkTest

import org.apache.spark.util.LongAccumulator

class BulletSparkMetricsSourceTest extends BulletSparkTest {
  behavior of "bullet spark metrics source"

  it should "No gauges have been registered" in {
    val accumulators = Map[String, LongAccumulator]()
    BulletSparkMetricsSource.register(ssc, accumulators, enabled = true)

    val source = ssc.sparkContext.env.metricsSystem.getSourcesByName("BulletSparkMetricsSource")

    source.size should equal(1)

    val bulletSparkSource = source.head.asInstanceOf[BulletSparkMetricsSource]

    bulletSparkSource.metricRegistry.getGauges().size should equal(0)
  }

  it should "all gauges have been registered and work correctly" in {
    val accumulators = Map[String, LongAccumulator](
      "a" -> sc.longAccumulator("a"),
      "b" -> sc.longAccumulator("b")
    )
    BulletSparkMetricsSource.register(ssc, accumulators, enabled = true)

    val source = ssc.sparkContext.env.metricsSystem.getSourcesByName("BulletSparkMetricsSource")

    source.size should equal(1)

    val bulletSparkSource = source.head.asInstanceOf[BulletSparkMetricsSource]

    bulletSparkSource.metricRegistry.getGauges().size should equal(2)

    accumulators("a").add(1L)
    accumulators("b").add(-1L)

    val gauges = bulletSparkSource.metricRegistry.getGauges()
    gauges.get("a").getValue should equal(1)
    gauges.get("b").getValue should equal(-1)
  }

  it should "No BulletSparkSource has been registered if disabled" in {
    val accumulators = Map[String, LongAccumulator](
      "a" -> sc.longAccumulator("a"),
      "b" -> sc.longAccumulator("b")
    )
    BulletSparkMetricsSource.register(ssc, accumulators, enabled = false)

    val source = ssc.sparkContext.env.metricsSystem.getSourcesByName("BulletSparkMetricsSource")

    source.size should equal(0)
  }
}
