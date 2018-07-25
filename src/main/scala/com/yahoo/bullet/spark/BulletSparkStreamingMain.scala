/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.spark.utils.BulletSparkConfig
import joptsimple.OptionParser

object BulletSparkStreamingMain {
  val CONFIGURATION_ARG = "bullet-spark-conf"
  val HELP_ARG = "help"
  val PARSER: OptionParser = new OptionParser() {
    {
      accepts(CONFIGURATION_ARG, "An optional configuration YAML file for Bullet Spark")
        .withOptionalArg()
        .describedAs("Configuration file used to override Bullet Spark's default settings")
      accepts(HELP_ARG, "Show this help message")
        .withOptionalArg()
        .describedAs("Print help message")
      allowsUnrecognizedOptions()
    }
  }

  def main(args: Array[String]): Unit = {
    val options = PARSER.parse(args: _*)
    if (options.has(HELP_ARG)) {
      PARSER.printHelpOn(System.out)
    } else {
      val bulletSparkConfigPath = options.valueOf(CONFIGURATION_ARG).asInstanceOf[String]
      val job = new BulletSparkStreamingBaseJob()
      val config = new BulletSparkConfig(bulletSparkConfigPath)
      val ssc = job.getOrCreateContext(config)
      ssc.start()
      ssc.awaitTermination()
    }
  }
}
