/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.io.File

import com.yahoo.bullet.common.SerializerDeserializer
import com.yahoo.bullet.pubsub.{Metadata, PubSubMessage}
import com.yahoo.bullet.query.QueryUtils.makeFieldFilterQuery
import com.yahoo.bullet.spark.utils.BulletSparkConfig

import org.apache.commons.io.FileUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class BulletSparkStreamingBaseJobTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually {
  private val metadata = new Metadata()
  // Override waiting time to 10s since it's a spark streaming with checkpoint.
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  behavior of "The bullet spark streaming job"

  before {
    // Delete target/spark-test directory.
    FileUtils.deleteDirectory(new File("target/spark-test"))
  }

  it should "run end to end successfully" in {
    System.getProperties.setProperty("spark.master", "local[4]")
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val job = new BulletSparkStreamingBaseJob()
    val ssc = job.getOrCreateContext(config)
    ssc.start()

    val query = makeFieldFilterQuery("fake_field")
    val message = new PubSubMessage("42", SerializerDeserializer.toBytes(query), metadata)
    CustomSubscriber.subscriber.open()
    CustomSubscriber.subscriber.addMessages(message)

    eventually {
      CustomPublisher.publisher.sent.length should equal(1)
      ssc.stop(stopSparkContext = true, stopGracefully = false)
    }
  }

  it should "run end to end successfully when recovering from checkpoint is enabled" in {
    System.getProperties.setProperty("spark.master", "local[4]")
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val job = new BulletSparkStreamingBaseJob()

    config.set("bullet.spark.recover.from.checkpoint.enable", true)

    val ssc1 = job.getOrCreateContext(config)
    ssc1.start()
    val query = makeFieldFilterQuery("fake_field")
    val message = new PubSubMessage("42", SerializerDeserializer.toBytes(query), metadata)
    CustomSubscriber.subscriber.open()
    CustomSubscriber.subscriber.addMessages(message)
    
    ssc1.stop(stopSparkContext = true, stopGracefully = false)
    ResultPublisher.clearInstance()
    BulletSparkConfig.clearInstance()

    val ssc2 = job.getOrCreateContext(config)
    ssc2.start()

    eventually {
      CustomPublisher.publisher.sent.length should equal(1)
      ssc2.stop(stopSparkContext = true, stopGracefully = false)
    }
  }
}
