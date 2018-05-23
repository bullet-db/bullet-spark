/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.data

import scala.collection.mutable.ListBuffer

import com.yahoo.bullet.pubsub.PubSubMessage

object BulletResultType {

  sealed trait BulletResultType

  case object CLIP extends BulletResultType

  case object FEEDBACK extends BulletResultType
}

/**
 * Class to wrap the result being emitted from Bullet.
 */
class BulletResult {
  val results: ListBuffer[(PubSubMessage, BulletResultType.BulletResultType)] = ListBuffer.empty

  def add(message: PubSubMessage): Unit = {
    results += ((message, BulletResultType.CLIP))
  }

  def addFeedback(message: PubSubMessage): Unit = {
    results += ((message, BulletResultType.FEEDBACK))
  }
}
