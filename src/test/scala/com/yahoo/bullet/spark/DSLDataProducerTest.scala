/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.dsl.BulletDSLConfig
import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.utils.BulletSparkConfig

import scala.collection.mutable.ListBuffer

class DSLDataProducerTest extends BulletSparkTest {
  behavior of "The dsl bullet record producer"

  it should "throw exception on failing to create a producer" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set(BulletSparkConfig.DSL_DATA_PRODUCER_ENABLE, "throw")
    val dslConfig = new BulletDSLConfig(config)
    the[RuntimeException] thrownBy {
      DataProducer.getProducer(config)
    } should have message "Can not create BulletRecordProducer instance."
  }

  it should "create a dsl producer successfully" in {
    val config = new BulletSparkConfig("src/test/resources/test_dsl_config.yaml")
    config.set(BulletSparkConfig.DSL_DATA_PRODUCER_ENABLE, true)
    config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.spark.MockConnector")
    config.set(BulletDSLConfig.DESERIALIZER_CLASS_NAME, "com.yahoo.bullet.dsl.deserializer.IdentityDeserializer")
    config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, "com.yahoo.bullet.dsl.converter.MapBulletRecordConverter")

    val producer = DataProducer.getProducer(config)

    producer shouldBe a [DSLDataProducer]

    val outputCollector = ListBuffer.empty[Array[BulletRecord[_ <: java.io.Serializable]]]

    val outputStream = producer.getBulletRecordStream(ssc, config)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      outputCollector.flatten.toList should equal(List.empty)
      outputCollector.flatten.toList should not equal List.empty
      //outputCollector.flatten.count(_.typedGet("field").getValue != "fake_field") should equal(0)
    }
  }

  it should "create a dsl producer successfully 2" in {
    val config = new BulletSparkConfig("src/test/resources/test_dsl_config.yaml")
    config.set(BulletSparkConfig.DSL_DATA_PRODUCER_ENABLE, true)
    config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.spark.MockConnector")
    config.set(BulletDSLConfig.DESERIALIZER_CLASS_NAME, "com.yahoo.bullet.dsl.deserializer.IdentityDeserializer")
    config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, "com.yahoo.bullet.dsl.converter.MapBulletRecordConverter")

    val producer = DataProducer.getProducer(config)

    producer shouldBe a [DSLDataProducer]

    val outputCollector = ListBuffer.empty[Array[BulletRecord[_ <: java.io.Serializable]]]

    val outputStream = producer.getBulletRecordStream(ssc, config)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      outputCollector.flatten.toList should equal(List.empty)
      outputCollector.flatten.toList should not equal List.empty
      //outputCollector.flatten.count(_.typedGet("field").getValue != "fake_field") should equal(0)
    }
  }

  it should "create a dsl producer successfully 3" in {
    val config = new BulletSparkConfig("src/test/resources/test_dsl_config.yaml")
    config.set(BulletSparkConfig.DSL_DATA_PRODUCER_ENABLE, true)
    config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.spark.MockConnector")
    config.set(BulletDSLConfig.DESERIALIZER_CLASS_NAME, "com.yahoo.bullet.dsl.deserializer.IdentityDeserializer")
    config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, "com.yahoo.bullet.dsl.converter.MapBulletRecordConverter")

    val producer = DataProducer.getProducer(config)

    producer shouldBe a [DSLDataProducer]

    val outputCollector = ListBuffer.empty[Array[BulletRecord[_ <: java.io.Serializable]]]

    val outputStream = producer.getBulletRecordStream(ssc, config)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      outputCollector.flatten.toList should equal(List.empty)
      outputCollector.flatten.toList should not equal List.empty
      //outputCollector.flatten.count(_.typedGet("field").getValue != "fake_field") should equal(0)
    }
  }
}
