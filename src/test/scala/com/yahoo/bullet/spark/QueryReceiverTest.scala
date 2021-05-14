/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import scala.collection.mutable.ListBuffer

import com.yahoo.bullet.pubsub.{Metadata, PubSubMessage}
import com.yahoo.bullet.spark.utils.BulletSparkConfig

class QueryReceiverTest extends BulletSparkTest {

  behavior of "The query receiver"

  it should "throw exception on failing to create a pubsub" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set("bullet.pubsub.class.name", "fake.class")
    the[RuntimeException] thrownBy {
      new QueryReceiver(config).onStart()
    } should have message "Cannot create PubSub instance or a Subscriber for it."
  }

  it should "output messages which are received" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val queryReceiver = new QueryReceiver(config)

    val messageA = new PubSubMessage("42", "This is a PubSubMessage", new Metadata)
    val messageB = new PubSubMessage("43", "This is also a PubSubMessage", new Metadata)
    CustomSubscriber.subscriber.open()
    CustomSubscriber.subscriber.addMessages(messageA, messageB)

    val outputCollector = ListBuffer.empty[Array[PubSubMessage]]

    val queryStream = ssc.receiverStream(queryReceiver)
    queryStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      outputCollector.flatten.toList should equal(List(messageA, messageB))
    }
  }

  it should "output non-null messages and ignore null messages" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val queryReceiver = new QueryReceiver(config)

    val messageA = new PubSubMessage("42", "This is a PubSubMessage", new Metadata)
    CustomSubscriber.subscriber.open()
    CustomSubscriber.subscriber.addMessages(messageA, null)

    val outputCollector = ListBuffer.empty[Array[PubSubMessage]]

    val queryStream = ssc.receiverStream(queryReceiver)
    queryStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      outputCollector.flatten.toList should equal(List(messageA))
    }
  }

  it should "output empty results when the pubsub throws exception" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val queryReceiver = new QueryReceiver(config)

    CustomSubscriber.subscriber.closeCalled = false
    CustomSubscriber.subscriber.closed = true

    val outputCollector = ListBuffer.empty[Array[PubSubMessage]]

    val queryStream = ssc.receiverStream(queryReceiver)
    queryStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      CustomSubscriber.subscriber.closeCalled should equal(true)
    }
  }
}
