/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.dsl.BulletDSLConfig
import com.yahoo.bullet.dsl.converter.BulletRecordConverter
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer
import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.utils.BulletSparkConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * The DSLDataProducer gets a Bullet record stream by deserializing and converting the output of a DSLReceiver.
  */
class DSLDataProducer extends DataProducer {
  override def getBulletRecordStream(ssc: StreamingContext, bulletSparkConfig: BulletSparkConfig): DStream[BulletRecord[_ <: java.io.Serializable]] = {
    val config = new BulletDSLConfig(bulletSparkConfig)
    val receiver = new DSLReceiver(config)
    val converter = BulletRecordConverter.from(config)
    val dslDeserializerEnable = config.get(BulletSparkConfig.DSL_DESERIALIZER_ENABLE).asInstanceOf[Boolean]
    if (dslDeserializerEnable) {
      val deserializer = BulletDeserializer.from(config)
      ssc.receiverStream(receiver).map(deserializer.deserialize).map(converter.convert)
    } else {
      ssc.receiverStream(receiver).map(converter.convert)
    }
  }
}
