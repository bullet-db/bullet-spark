/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package org.apache.spark

import java.util.Date

import org.apache.spark.streaming.Duration

/**
 * A `Clock` whose time can be manually set and modified. Its reported time does not change
 * as time elapses, but only as its time is modified by callers. This is mainly useful for
 * testing.
 *
 * Unfortunately, the Clock interface has a package protected visibility limited to the
 * org.apache.spark package. But we can workaround this by placing our own implementation
 * extending the interface in this same package.
 *
 * This is sourced from https://github.com/raphaelbrugier/spark-testing-example.
 */
class FixedClock(var currentTime: Long) extends org.apache.spark.util.Clock {
  def this() = this(0L)

  def setCurrentTime(time: Date): Unit = synchronized {
    currentTime = time.getTime
    notifyAll()
  }

  def addTime(duration: Duration): Unit = synchronized {
    currentTime += duration.milliseconds
    notifyAll()
  }

  override def getTimeMillis(): Long = synchronized {
    currentTime
  }

  override def waitTillTime(targetTime: Long): Long = synchronized {
    while (currentTime < targetTime) {
      wait(10)
    }
    getTimeMillis()
  }
}
