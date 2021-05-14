/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.data

import com.yahoo.bullet.pubsub.Metadata
import com.yahoo.bullet.querying.RunningQuery

/**
 * Class to wrap the result from the FilterStreaming phase, including the RunningQuery.
 *
 * @constructor Creates a data wrapper the filter data.
 * @param metadata     The metadata information associated with the query.
 * @param runningQuery The running query instance.
 * @param data         The byte array representing the filter phase result.
 */
class FilterResultData(metadata: Metadata, val runningQuery: RunningQuery, val data: Array[Byte])
  extends BulletData(metadata) {
  /**
   * @constructor Create a Bullet filter result data.
   * @param runningQueryData The [[com.yahoo.bullet.spark.data.RunningQueryData]] object.
   * @param data             The byte array representing the filter phase result.
   */
  def this(runningQueryData: RunningQueryData, data: Array[Byte]) {
    this(runningQueryData.metadata, runningQueryData.runningQuery, data)
  }
}
