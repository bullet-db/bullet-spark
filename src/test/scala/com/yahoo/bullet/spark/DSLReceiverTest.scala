/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.util.Arrays
import java.util.Collections

import com.yahoo.bullet.dsl.BulletDSLConfig
import com.yahoo.bullet.spark.utils.BulletSparkConfig
import org.scalatest.time.{Seconds, Span}

import scala.collection.mutable.ListBuffer

class DSLReceiverTest extends BulletSparkTest {
  behavior of "The dsl receiver"

  it should "throw exception on failing to create a connector" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    the[RuntimeException] thrownBy {
      new DSLReceiver(new BulletDSLConfig(config)).onStart()
    } should have message "Cannot create BulletConnector instance or initialize it."
  }

  it should "output objects which are received" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.spark.MockConnector")

    val dslReceiver = new DSLReceiver(new BulletDSLConfig(config))

    val outputCollector = ListBuffer.empty[Array[AnyRef]]

    val dataStream = ssc.receiverStream(dslReceiver)
    dataStream.foreachRDD(rdd => outputCollector += rdd.collect())

    MockConnector.data += Arrays.asList("hello", "world")
    MockConnector.data += Collections.emptyList()
    MockConnector.data += Collections.singletonList("!")

    ssc.start()

    eventually (timeout(Span(5, Seconds))) {
      wait1second()
      outputCollector.flatten should equal(List("hello", "world", "!"))
    }
  }

  it should "close the connector when the connector throws an exception" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.spark.MockConnector")

    val dslReceiver = new DSLReceiver(new BulletDSLConfig(config))

    val outputCollector = ListBuffer.empty[Array[AnyRef]]

    val dataStream = ssc.receiverStream(dslReceiver)
    dataStream.foreachRDD(rdd => outputCollector += rdd.collect())

    MockConnector.closeCalled = false

    ssc.start()

    eventually (timeout(Span(5, Seconds))) {
      MockConnector.closed = true
      wait1second()
      MockConnector.closeCalled should equal(true)
    }
  }
}
