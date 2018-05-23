/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.data

import com.yahoo.bullet.pubsub.Metadata
import com.yahoo.bullet.pubsub.Metadata.Signal

/**
 * Class to wrap Metadata object with a Signal.
 *
 * @constructor Create a data wrapper for metadata with signals.
 * @param metadata The metadata information associated with the query.
 * @param signal   The signal associated with the metadata.
 */
class BulletSignalData(metadata: Metadata, val signal: Signal) extends BulletData(metadata)
