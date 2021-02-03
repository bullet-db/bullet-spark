/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

class BulletSparkMetricsTest extends BulletSparkTest {
  behavior of "bullet spark metrics"
/*
  it should "work correctly when enabling metrics" in {

    val metrics = new BulletSparkMetrics(ssc, true)

    BulletSparkMetrics.newQueryReceived(metrics)

    metrics.metricsMap(BulletSparkMetrics.QUERIES_RECEIVED_METRIC).value should equal(1)
    metrics.metricsMap(BulletSparkMetrics.QUERIES_RUNNING_METRIC).value should equal(1)
    metrics.metricsMap(BulletSparkMetrics.QUERIES_DONE_METRIC).value should equal(0)

    BulletSparkMetrics.queryFinished(metrics)

    metrics.metricsMap(BulletSparkMetrics.QUERIES_RECEIVED_METRIC).value should equal(1)
    metrics.metricsMap(BulletSparkMetrics.QUERIES_RUNNING_METRIC).value should equal(0)
    metrics.metricsMap(BulletSparkMetrics.QUERIES_DONE_METRIC).value should equal(1)

    metrics.increase("non existing metric")
    metrics.decrease("non existing metric")

    metrics.metricsMap(BulletSparkMetrics.QUERIES_RECEIVED_METRIC).value should equal(1)
    metrics.metricsMap(BulletSparkMetrics.QUERIES_RUNNING_METRIC).value should equal(0)
    metrics.metricsMap(BulletSparkMetrics.QUERIES_DONE_METRIC).value should equal(1)
  }

  it should "work correctly when disabling metrics" in {

    val metrics = new BulletSparkMetrics(ssc, false)

    BulletSparkMetrics.newQueryReceived(metrics)

    metrics.metricsMap should equal(null)

    BulletSparkMetrics.queryFinished(metrics)

    metrics.metricsMap should equal(null)

    metrics.increase("non existing metric")
    metrics.decrease("non existing metric")

    metrics.metricsMap should equal(null)
  }
  */
}
