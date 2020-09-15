/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import scala.collection.mutable.ListBuffer

import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.utils.BulletSparkConfig

class DataProducerTest extends BulletSparkTest {
  behavior of "The bullet record producer"

  it should "throw exception on failing to create a producer" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set("bullet.spark.data.producer.class.name", "fake.class")
    the[RuntimeException] thrownBy {
      DataProducer.getProducer(config)
    } should have message "Can not create BulletRecordProducer instance."
  }

  it should "create a producer successfully" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val producer = DataProducer.getProducer(config)

    val outputCollector = ListBuffer.empty[Array[BulletRecord[_ <: java.io.Serializable]]]

    val outputStream = producer.getBulletRecordStream(ssc, config)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      outputCollector.flatten should not equal List.empty
      outputCollector.flatten.count(_.get("field") != "fake_field") should equal(0)
    }
  }
}
