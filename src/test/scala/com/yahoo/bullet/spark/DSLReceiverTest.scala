/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.dsl.BulletDSLConfig
import com.yahoo.bullet.spark.utils.BulletSparkConfig

import scala.collection.mutable.ListBuffer

class DSLReceiverTest extends BulletSparkTest {
  behavior of "The dsl receiver"

  it should "throw exception on failing to create a connector" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.spark.MockConnector")
    config.set("DSLReceiverTest", true)
    the[RuntimeException] thrownBy {
      new DSLReceiver(new BulletDSLConfig(config)).onStart()
    } should have message "Cannot create BulletConnector instance or initialize it."
  }

  it should "output messages" in {
    //val config = new BulletSparkConfig("src/test/resources/test_dsl_config.yaml")
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.spark.MockConnector")

    val dslReceiver = new DSLReceiver(new BulletDSLConfig(config))

    val outputCollector = ListBuffer.empty[Array[AnyRef]]

    val dataStream = ssc.receiverStream(dslReceiver)
    dataStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    eventually {
      wait1second()
      outputCollector.flatten should not equal List.empty
    }
  }
}
