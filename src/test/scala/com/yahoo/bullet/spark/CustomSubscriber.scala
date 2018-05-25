/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import scala.collection.mutable.{ArrayBuffer, Queue}

import com.yahoo.bullet.pubsub.{PubSubException, PubSubMessage, Subscriber}

object CustomSubscriber {
  val subscriber = new CustomSubscriber
}

class CustomSubscriber extends Subscriber {
  private val queue = Queue.empty[PubSubMessage]
  private val received = ArrayBuffer.empty[PubSubMessage]
  private val committed = ArrayBuffer.empty[String]
  private val failed = ArrayBuffer.empty[String]
  var closed = false
  var closeCalled = false

  def addMessages(pubSubMessages: PubSubMessage*): Unit = {
    for (pubSubMessage <- pubSubMessages) {
      println("Message added to queue")
      queue += pubSubMessage
    }
  }

  def open() : Unit = {
    closed = false
  }

  @throws[PubSubException]
  override def receive: PubSubMessage = {
    if (closed) {
      throw new PubSubException("")
    }
    if (queue.isEmpty) {
      null
    } else {
      val message = queue.dequeue()
      received += message
      println("Message received")
      message
    }
  }

  override def close(): Unit = {
    closeCalled = true
    closed = true
  }

  override def commit(id: String, sequence: Int): Unit = {
    committed += id
  }

  override def fail(id: String, sequence: Int): Unit = {
    failed += id
  }
}
