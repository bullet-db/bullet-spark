/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.spark.utils.BulletSparkConfig

object BulletSparkStreamingMain {
  def main(args: Array[String]): Unit = {
    val job = new BulletSparkStreamingBaseJob()
    val config = new BulletSparkConfig()
    val ssc = job.getOrCreateContext(config)
    ssc.start()
    ssc.awaitTermination()
  }
}
