/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.dsl.BulletDSLConfig
import com.yahoo.bullet.dsl.connector.BulletConnector
import com.yahoo.bullet.spark.utils.BulletSparkLogger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Create a DSLReceiver from the given configuration.
  *
  * @param config The { @link BulletDSLConfig} to load settings from.
  */
class DSLReceiver(val config: BulletDSLConfig) extends Receiver[Object](StorageLevel.MEMORY_AND_DISK_SER) with BulletSparkLogger {
  private var connector: BulletConnector = _

  override def onStart(): Unit = {
    try {
      connector = BulletConnector.from(config)
      connector.initialize()
    } catch {
      case e: Exception =>
        throw new RuntimeException("Cannot create BulletConnector instance or initialize it.", e)
    }
    new Thread() {
      override def run(): Unit = {
        receive()
      }
    }.start()
    logger.info("DSL receiver started.")
  }

  override def onStop(): Unit = {
    if (connector != null) {
      connector.close()
      connector = null
    }
    logger.info("DSL receiver stopped.")
  }

  private def receive(): Unit = {
    while (!isStopped()) {
      try {
        val objects = connector.read()
        if (!objects.isEmpty) {
          store(objects.iterator())
        }
      } catch {
        case t: Throwable =>
          if (connector != null) {
            connector.close()
            connector = null
          }
          restart("Error receiving data.", t)
      }
    }
  }
}
