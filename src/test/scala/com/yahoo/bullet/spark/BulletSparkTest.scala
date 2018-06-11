/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.io.File

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.querying.RunningQuery
import com.yahoo.bullet.spark.utils.BulletSparkConfig
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.{Clock, Seconds, StreamingContext}
import org.apache.spark.{FixedClock, SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class NeverExpiringRunningQuery(id: String, queryString: String, config: BulletConfig)
  extends RunningQuery(id, queryString, config) {
  override def isTimedOut: Boolean = false
}

class ExpiredRunningQuery(id: String, queryString: String, config: BulletConfig)
  extends RunningQuery(id, queryString, config) {
  override def isTimedOut: Boolean = true
}

class CustomRunningQuery(id: String, queryString: String, config: BulletConfig, val expireAfter: Int)
  extends RunningQuery(id, queryString, config) {
  var i = 0

  override def isTimedOut: Boolean = {
    i += 1
    i >= expireAfter
  }
}

/**
 * Trait for bullet spark unit test.
 */
trait BulletSparkTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually {
  var sc: SparkContext = _
  var ssc: StreamingContext = _
  var fixedClock: FixedClock = _

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(2000, Millis)))

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BulletSparkTest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.clock", "org.apache.spark.FixedClock")
      .set("spark.default.parallelism", "1")
      .set("spark.streaming.blockInterval", "1ms")
      .set("spark.streaming.receiverRestartDelay", "1")

    ssc = new StreamingContext(sparkConf, Seconds(1))
    sc = ssc.sparkContext
    fixedClock = Clock.getFixedClock(ssc)
    ResultPublisher.clearInstance()
    BulletSparkConfig.clearInstance()

    // Delete target/spark-test directory.
    FileUtils.deleteDirectory(new File("target/spark-test"))
  }

  after {
    ssc.stop(stopSparkContext = true, stopGracefully = false)
  }

  def wait1second(): Unit = {
    fixedClock.addTime(Seconds(1))
  }
}
