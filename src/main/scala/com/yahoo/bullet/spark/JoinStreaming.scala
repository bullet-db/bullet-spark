/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.pubsub.Metadata.Signal
import com.yahoo.bullet.querying.{Querier, RateLimitError}
import com.yahoo.bullet.spark.data.{
  BulletData, BulletErrorData, BulletResult, BulletSignalData, FilterResultData, QuerierData, RunningQueryData
}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, State, StateSpec, StreamingContext, Time}

object JoinStreaming {
  /**
   * Join and aggregate the partial results stream.
   *
   * This method takes the [[com.yahoo.bullet.spark.data.BulletData]] stream coming from the QueryDataUnioning phase and
   * the FilterStreaming phase and emits the [[com.yahoo.bullet.spark.data.BulletResult]] stream to
   * the ResultEmitter phase. It combines all the partial results with the same key and emits the
   * [[com.yahoo.bullet.spark.data.BulletResult]] accordingly. It uses mapWithState to remember the intermediate
   * combined results.
   *
   * @param ssc                  The Spark streaming context.
   * @param partialResultsStream The [[com.yahoo.bullet.spark.data.BulletData]] partial results stream.
   * @param broadcastedConfig    The broadcasted [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] configuration
   *                             that has been validated.
   * @return A [[com.yahoo.bullet.spark.data.BulletResult]] stream which contains results to be emitted.
   */
  def join(ssc: StreamingContext, partialResultsStream: DStream[(String, BulletData)],
           broadcastedConfig: Broadcast[BulletSparkConfig]): DStream[(String, BulletResult)] = {
    val metrics = BulletSparkMetrics.getInstance(ssc, broadcastedConfig)
    val config = broadcastedConfig.value
    val duration = config.get(BulletSparkConfig.BATCH_DURATION_MS).asInstanceOf[Int]
    val durationMultiplier = config.get(BulletSparkConfig.JOIN_CHECKPOINT_DURATION_MULTIPLIER).asInstanceOf[Int]
    partialResultsStream.mapWithState(StateSpec.function(makeMapFunc(metrics, broadcastedConfig) _)).cache()
      .checkpoint(Durations.milliseconds(durationMultiplier * duration))
  }

  private def makeMapFunc(metrics: BulletSparkMetrics, broadcastedConfig: Broadcast[BulletSparkConfig])
                         (batchTime: Time, key: String, value: Option[BulletData],
                          state: State[QuerierData]): Option[(String, BulletResult)] = {
    // No timeout has been set. Value is not empty.
    val bulletData = value.get
    bulletData match {
      case signalData: BulletSignalData => onSignal(key, signalData, state, metrics)
      case _ if isDone(state) => None
      case errorData: BulletErrorData => onError(key, errorData, state, metrics)
      case runningQueryData: RunningQueryData =>
        onRunningQuery(batchTime, key, runningQueryData, state, broadcastedConfig, metrics)
      case filterResultData: FilterResultData =>
        onData(batchTime, key, filterResultData, state, broadcastedConfig, metrics)
    }
  }

  private def isDone(state: State[QuerierData]): Boolean = {
    state.exists() && state.get().finished
  }

  private def onSignal(key: String, signalData: BulletSignalData, state: State[QuerierData],
                       metrics: BulletSparkMetrics): Option[(String, BulletResult)] = {
    state.getOption() match {
      case Some(stateQuery) =>
        state.remove()
        if (stateQuery.finished) {
          None
        } else {
          val bulletResult = new BulletResult()
          bulletResult.add(BulletSparkUtils.createResultMessageWithSignal(key, stateQuery, signalData.signal))
          BulletSparkMetrics.queryFinished(metrics)
          Option(key, bulletResult)
        }
      case None => None
    }
  }

  private def onError(key: String, errorData: BulletErrorData, state: State[QuerierData],
                      metrics: BulletSparkMetrics): Option[(String, BulletResult)] = {
    val bulletResult = new BulletResult()
    val updatedState = state.getOption() match {
      case Some(querierData) =>
        // We have received a RateLimitError from the FilterStreaming phase.
        bulletResult.add(BulletSparkUtils.createRateLimitErrorMessage(key,
          querierData, errorData.errors.head.asInstanceOf[RateLimitError]))
        querierData
      case None =>
        // We have received other errors that were created at query parsing or initialization.
        bulletResult.add(BulletSparkUtils.createErrorMessage(key, errorData))
        new QuerierData(errorData.metadata, null, null)
    }

    updatedState.finished = true
    state.update(updatedState)
    bulletResult.addFeedback(BulletSparkUtils.createFeedbackMessage(key, Signal.KILL))
    BulletSparkMetrics.queryFinished(metrics)
    Option(key, bulletResult)
  }

  private def onRunningQuery(batchTime: Time, key: String, runningQueryData: RunningQueryData,
                             state: State[QuerierData], broadcastedConfig: Broadcast[BulletSparkConfig],
                             metrics: BulletSparkMetrics): Option[(String, BulletResult)] = {
    val filterResultData = new FilterResultData(runningQueryData, Array[Byte]())
    onData(batchTime, key, filterResultData, state, broadcastedConfig, metrics)
  }

  private def onData(batchTime: Time, key: String, filterResultData: FilterResultData,
                     state: State[QuerierData], broadcastedConfig: Broadcast[BulletSparkConfig],
                     metrics: BulletSparkMetrics): Option[(String, BulletResult)] = {
    val querierState = state.getOption().getOrElse(
      new QuerierData(filterResultData.metadata,
        BulletSparkUtils.createBulletQuerier(filterResultData.runningQuery, Querier.Mode.ALL, broadcastedConfig), null))
    val bulletResults = new BulletResult()

    // If this is the first data being received for this batch, check the query and emit results accordingly if it
    // should be emitted before consuming data.
    if (!batchTime.equals(querierState.currentBatch)) {
      emitResults(key, querierState, bulletResults, metrics)
    }

    // If the query is not finished and data is not empty, consume data and check if it should be emitted.
    if (!querierState.finished && filterResultData.data.nonEmpty) {
      querierState.querier.combine(filterResultData.data)
      emitResults(key, querierState, bulletResults, metrics)
    }

    querierState.currentBatch = batchTime
    state.update(querierState)
    if (bulletResults.results.isEmpty) {
      None
    } else {
      Option(key, bulletResults)
    }
  }

  private def emitResults(key: String, querierData: QuerierData, bulletResult: BulletResult,
                          metrics: BulletSparkMetrics): Unit = {
    val querier = querierData.querier
    if (querier.isDone) {
      querierData.finished = true
      bulletResult.add(BulletSparkUtils.createResultMessageWithSignal(key, querierData, Signal.COMPLETE))
      bulletResult.addFeedback(BulletSparkUtils.createFeedbackMessage(key, Signal.COMPLETE))
      BulletSparkMetrics.queryFinished(metrics)
    } else {
      if (querier.isClosed) {
        bulletResult.add(BulletSparkUtils.createResultMessage(key, querierData))
        querier.reset()
      }
      if (querier.isExceedingRateLimit) {
        querierData.finished = true
        val message = BulletSparkUtils.createRateLimitErrorMessage(
          key, querierData, querier.getRateLimitError)
        bulletResult.add(message)
        bulletResult.addFeedback(BulletSparkUtils.createFeedbackMessage(key, Signal.KILL))
        BulletSparkMetrics.queryFinished(metrics)
      }
    }
  }
}
