/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.spark.data.{BulletResult, BulletResultType}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

object ResultEmitter {
  /**
   * Emits the results back through the PubSub.
   *
   * @param outputDStream        The output [[com.yahoo.bullet.spark.data.BulletResult]] stream.
   * @param broadcastedPublisher The broadcasted [[com.yahoo.bullet.spark.ResultPublisher]] instance.
   */
  def emit(outputDStream: DStream[(String, BulletResult)], broadcastedPublisher: Broadcast[ResultPublisher]): Unit = {
    outputDStream.foreachRDD(_.foreach(record => publish(broadcastedPublisher.value, record._2)))
  }

  private def publish(publisher: ResultPublisher, bulletResult: BulletResult): Unit = {
    bulletResult.results.foreach(result => {
      result._2 match {
        case BulletResultType.CLIP => publisher.publish(result._1)
        case BulletResultType.FEEDBACK => publisher.publishFeedback(result._1)
      }
    })
  }
}
