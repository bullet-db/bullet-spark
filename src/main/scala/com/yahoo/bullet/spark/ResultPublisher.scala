/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.util.HashMap

// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.pubsub.{PubSub, PubSubException, PubSubMessage, Publisher}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkLogger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

object ResultPublisher extends BulletSparkLogger {
  @volatile private var instance: Broadcast[ResultPublisher] = _

  def getInstance(ssc: StreamingContext,
                  broadcastedConfig: Broadcast[BulletSparkConfig]): Broadcast[ResultPublisher] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val f = () => {
            try {
              val config = broadcastedConfig.value
              val pubSub = PubSub.from(config)
              val publisher = pubSub.getPublisher
              logger.info("Setup PubSub: {} with Publisher: {}", Array(pubSub, publisher): _*)

              // Setup Pubsub for feedback loop.
              val overrides =
                config.get(BulletSparkConfig.LOOP_PUBSUB_OVERRIDES).asInstanceOf[HashMap[String, AnyRef]].asScala
              logger.info("Loaded PubSub overrides: {}", overrides)
              val modified = new BulletConfig()
              overrides.foreach(item => modified.set(item._1, item._2))
              pubSub.switchContext(PubSub.Context.QUERY_SUBMISSION, modified)

              val publisherForLoop = pubSub.getPublisher
              logger.info("Setup Publisher for looping: {}", publisherForLoop)
              (publisher, publisherForLoop)
            } catch {
              case e: PubSubException =>
                throw new RuntimeException("Cannot create PubSub instance or a Publisher for it.", e)
            }
          }
          instance = ssc.sparkContext.broadcast(new ResultPublisher(f))
        }
      }
    }
    instance
  }

  // For testing.
  private[spark] def clearInstance(): Unit = synchronized {
    instance = null
  }
}

/**
 * Utility class to publish results.
 *
 * @constructor Create a ResultPublisher.
 * @param createPublisher The function to generate publishers.
 */
class ResultPublisher(createPublisher: () => (Publisher, Publisher)) extends BulletSparkLogger {
  private lazy val (publisher, publisherForLoop): (Publisher, Publisher) = createPublisher()

  /**
   * Publishes a PubSubMessage by publisher.
   *
   * @param message The PubSubMessage to be published.
   */
  def publish(message: PubSubMessage): Unit = synchronized {
    try {
      publisher.send(message)
      logger.debug("Publishing: {}", message)
    } catch {
      case e: PubSubException => logger.error(e.getMessage)
    }
  }

  /**
   * Publishes a feedback PubSubMessage by publisher.
   *
   * @param message The PubSubMessage to be published.
   */
  def publishFeedback(message: PubSubMessage): Unit = synchronized {
    try {
      publisherForLoop.send(message)
      logger.debug("Publishing feedback: {}", message)
    } catch {
      case e: PubSubException => logger.error(e.getMessage)
    }
  }
}
