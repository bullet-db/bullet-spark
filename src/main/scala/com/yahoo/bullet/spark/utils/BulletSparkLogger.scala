/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Utility trait for logging.
 */
trait BulletSparkLogger {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
}
