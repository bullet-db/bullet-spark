/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.pubsub.{PubSub, PubSubException, Publisher, Subscriber}

@throws[PubSubException]
class MockPubSub(val configuration: BulletConfig) extends PubSub(configuration) {
  CustomPublisher.publisher.reset()
  CustomPublisher.feedbackPublisher.reset()

  override def getPublisher: Publisher = {
    context match {
      case PubSub.Context.QUERY_PROCESSING => CustomPublisher.publisher
      case PubSub.Context.QUERY_SUBMISSION => CustomPublisher.feedbackPublisher
    }
  }

  override def getSubscriber: Subscriber = CustomSubscriber.subscriber

  override def getPublishers(n: Int) = throw new UnsupportedOperationException

  override def getSubscribers(n: Int) = throw new UnsupportedOperationException
}
