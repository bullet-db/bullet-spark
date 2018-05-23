/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.data

import com.yahoo.bullet.pubsub.Metadata
import com.yahoo.bullet.querying.RunningQuery

/**
 * Class to wrap the RunningQuery and Metadata.
 *
 * @constructor Creates a data wrapper for a running query and metadata.
 * @param metadata     The metadata information associated with the query.
 * @param runningQuery The running query instance.
 */
class RunningQueryData(metadata: Metadata, val runningQuery: RunningQuery) extends BulletData(metadata)
