/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.data

import com.yahoo.bullet.pubsub.Metadata

/**
 * Class for storing arbitrary data for passing between the various Spark stages. It also stores the Metadata for
 * the query this data is intended for.
 *
 * It's the parent class for all data.
 *
 * @constructor Create an instance of the data to pass between stages.
 * @param metadata The metadata information associated with the query.
 * @param finished The Boolean to indicate whether query has been finished.
 */
abstract class BulletData(val metadata: Metadata, var finished: Boolean = false)
