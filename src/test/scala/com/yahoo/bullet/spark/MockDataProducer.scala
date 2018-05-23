/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.utils.BulletSparkConfig
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver

class MockDataReceiver(val config: BulletSparkConfig)
  extends Receiver[BulletRecord](StorageLevel.MEMORY_AND_DISK_SER) {
  override def onStart(): Unit = {
    new Thread() {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {}

  private def receive(): Unit = {
    while (!isStopped()) {
      val record = new BulletRecord()
      record.setString("field", "fake_field")
      try {
        store(record)
        Thread.sleep(1000)
      } catch {
        case _: Exception =>
      }
    }
  }
}

class MockDataProducer extends DataProducer {
  override def getBulletRecordStream(ssc: StreamingContext, config: BulletSparkConfig): DStream[BulletRecord] = {
    // Bullet record input stream.
    val bulletReceiver = new MockDataReceiver(config)
    ssc.receiverStream(bulletReceiver).asInstanceOf[DStream[BulletRecord]]
  }
}
