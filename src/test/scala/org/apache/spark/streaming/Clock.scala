/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package org.apache.spark.streaming

import org.apache.spark.FixedClock

/**
 * An utility method to workaround the private accessibility of the Clock in the StreamingContext
 * by placing our own implementation in this same package.
 *
 * This is sourced from https://github.com/raphaelbrugier/spark-testing-example.
 */
object Clock {
  def getFixedClock(ssc: StreamingContext): FixedClock = {
    ssc.scheduler.clock.asInstanceOf[FixedClock]
  }
}
