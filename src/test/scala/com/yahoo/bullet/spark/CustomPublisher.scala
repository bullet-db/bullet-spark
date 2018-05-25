/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import scala.collection.mutable.ArrayBuffer

import com.yahoo.bullet.pubsub.{PubSubException, PubSubMessage, Publisher}

object CustomPublisher {
  val publisher = new CustomPublisher
  val feedbackPublisher = new CustomPublisher
}

class CustomPublisher extends Publisher {
  val sent: ArrayBuffer[PubSubMessage] = ArrayBuffer.empty
  private var closed = false

  def reset() : Unit = {
    sent.clear()
    closed = false
  }

  @throws[PubSubException]
  override def send(message: PubSubMessage): Unit = {
    if (closed || message == null) {
      throw new PubSubException("")
    }
    sent += message
    println("Message sent")
  }

  override def close(): Unit = {
    closed = true
  }
}
