/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import scala.collection.mutable.ArrayBuffer

import com.yahoo.bullet.pubsub.{PubSub, PubSubException, PubSubMessage, Subscriber}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkLogger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * QueryReceiver receives queries from the PubSub and feeds it to the rest of the DAG.
 *
 * @constructor Create a QueryReceiver instance that takes a configuration.
 * @param config The [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] to load settings from.
 */
class QueryReceiver(val config: BulletSparkConfig)
  extends Receiver[PubSubMessage](StorageLevel.MEMORY_AND_DISK_SER) with BulletSparkLogger {
  private val blocks = ArrayBuffer.empty[PubSubMessage]
  private var subscriber: Subscriber = _
  private val blockSize = config.get(BulletSparkConfig.QUERY_BLOCK_SIZE).asInstanceOf[Int]

  override def onStart(): Unit = {
    try {
      val pubSub = PubSub.from(config)
      subscriber = pubSub.getSubscriber
      logger.info("Setup PubSub: {} with Subscriber: {}", Array(pubSub, subscriber): _*)
    } catch {
      case e: PubSubException =>
        throw new RuntimeException("Cannot create PubSub instance or a Subscriber for it.", e)
    }
    new Thread() {
      override def run(): Unit = {
        receive()
      }
    }.start()
    logger.info("Query receiver started.")
  }

  override def onStop(): Unit = {
    subscriber.close()
    logger.info("Query receiver stopped.")
  }

  private def receive(): Unit = {
    try {
      while (!isStopped()) {
        val message = subscriber.receive()
        if (message != null) {
          blocks += message
          logger.debug("Received a message: {}", message)
        }
        if (blocks.size >= blockSize) {
          store(blocks)
          blocks.foreach(m => subscriber.commit(m.getId))
          blocks.clear()
        }
      }
    } catch {
      case t: Throwable =>
        subscriber.close()
        restart("Error receiving data", t)
    }
  }
}
