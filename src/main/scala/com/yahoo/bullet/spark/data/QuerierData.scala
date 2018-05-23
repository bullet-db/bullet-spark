/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.data

import com.yahoo.bullet.pubsub.Metadata
import com.yahoo.bullet.querying.Querier
import org.apache.spark.streaming.Time

/**
 * Class to wrap a Bullet Querier instance with data and stores the batch time in addition.
 *
 * @constructor Create a data wrapper for a Querier.
 * @param metadata     The metadata information associated with the query that the querier is computing.
 * @param querier      The Bullet Querier instance.
 * @param currentBatch The current batch time for this query.
 */
class QuerierData(metadata: Metadata, val querier: Querier, var currentBatch: Time) extends BulletData(metadata)
