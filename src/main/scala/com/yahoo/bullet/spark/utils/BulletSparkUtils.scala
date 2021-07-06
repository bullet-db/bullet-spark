/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.utils

// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on

import com.yahoo.bullet.common.BulletError
import com.yahoo.bullet.pubsub.Metadata.Signal
import com.yahoo.bullet.pubsub.{Metadata, PubSubMessage}
import com.yahoo.bullet.querying.{Querier, RateLimitError, RunningQuery}
import com.yahoo.bullet.result.{Clip, Meta}
import com.yahoo.bullet.spark.data.{BulletData, BulletErrorData, BulletSignalData, QuerierData, RunningQueryData}
import org.apache.spark.broadcast.Broadcast

/**
 * Utility functions for use across the project.
 */
object BulletSparkUtils {
  /**
   * Creates a [[com.yahoo.bullet.spark.data.BulletData]] instance from a PubSubMessage.
   */
  def createBulletData(pubSubMessage: PubSubMessage, broadcastedConfig: Broadcast[BulletSparkConfig]): BulletData = {
    val id = pubSubMessage.getId
    val metadata = pubSubMessage.getMetadata
    val config = broadcastedConfig.value
    try {
      val signal: Metadata.Signal = if (metadata == null) null else metadata.getSignal
      if (signal != null && signal == Metadata.Signal.KILL || signal == Metadata.Signal.COMPLETE) {
        new BulletSignalData(metadata, signal)
      } else {
        val query = pubSubMessage.getContentAsQuery
        val runningQuery = new RunningQuery(id, query, metadata)
        new RunningQueryData(metadata, runningQuery)
      }
    } catch {
      case e: RuntimeException =>
        // Includes JSONParseException.
        new BulletErrorData(metadata, BulletError.makeError("Failed to initialize query for request " + id + ": " + e.getMessage, ""))
    }
  }

  /**
   * Creates a Querier instance from a RunningQuery and a broadcasted
   * [[com.yahoo.bullet.spark.utils.BulletSparkConfig]].
   */
  def createBulletQuerier(runningQuery: RunningQuery, mode: Querier.Mode,
                          broadcastedConfig: Broadcast[BulletSparkConfig]): Querier = {
    new Querier(mode, runningQuery, broadcastedConfig.value)
  }

  /**
   * Creates a Querier instance from a [[com.yahoo.bullet.spark.data.RunningQueryData]] and a broadcasted
   * [[com.yahoo.bullet.spark.utils.BulletSparkConfig]].
   */
  def createBulletQuerier(runningQueryData: RunningQueryData, mode: Querier.Mode,
                          broadcastedConfig: Broadcast[BulletSparkConfig]): Querier = {
    createBulletQuerier(runningQueryData.runningQuery, mode, broadcastedConfig)
  }

  /**
   * Creates an incremental result PubSubMessage from a [[com.yahoo.bullet.spark.data.QuerierData]].
   */
  def createResultMessage(id: String, querierData: QuerierData): PubSubMessage = {
    val clip = querierData.querier.getResult
    new PubSubMessage(id, clip.asJSON, querierData.metadata)
  }

  /**
   * Creates a final result PubSubMessage with the given signal from a [[com.yahoo.bullet.spark.data.QuerierData]].
   */
  def createResultMessageWithSignal(id: String, querierData: QuerierData, signal: Signal): PubSubMessage = {
    val clip = querierData.querier.finish()
    new PubSubMessage(id, clip.asJSON, withSignal(querierData.metadata, signal))
  }

  /**
   * Creates a result PubSubMessage for queries that have errored using a
   * [[com.yahoo.bullet.spark.data.BulletErrorData]].
   */
  def createErrorMessage(id: String, errorData: BulletErrorData): PubSubMessage = {
    val clip = Clip.of(Meta.of(errorData.errors.asJava))
    new PubSubMessage(id, clip.asJSON, withSignal(errorData.metadata, Signal.FAIL))
  }

  /**
   * Creates a result PubSubMessage for queries that have been rate-limited using a
   * [[com.yahoo.bullet.spark.data.QuerierData]].
   */
  def createRateLimitErrorMessage(id: String, querierData: QuerierData, error: RateLimitError): PubSubMessage = {
    val meta = error.makeMeta()
    val clip = querierData.querier.finish()
    clip.getMeta.merge(meta)
    new PubSubMessage(id, clip.asJSON, withSignal(querierData.metadata, Signal.FAIL))
  }

  /**
   * Create a feedback PubSubMessage for signals.
   */
  def createFeedbackMessage(id: String, signal: Signal): PubSubMessage = {
    new PubSubMessage(id, signal)
  }

  private def withSignal(metadata: Metadata, signal: Signal): Metadata = {
    if (metadata == null) {
      new Metadata(signal, null)
    } else {
      val copy = metadata.copy()
      copy.setSignal(signal)
      copy
    }
  }
}
