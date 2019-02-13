/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.utils

import java.util.HashMap

import scala.compat.java8.FunctionConverters.{asJavaFunction, asJavaPredicate}

import com.yahoo.bullet.common.{BulletConfig, Config, Validator}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

object BulletSparkConfig {
  val QUERY_BLOCK_SIZE = "bullet.spark.receiver.query.block.size"
  val QUERY_COALESCE_PARTITIONS = "bullet.spark.receiver.query.coalesce.partitions"
  val BATCH_DURATION_MS = "bullet.spark.batch.duration.ms"

  val DSL_DATA_PRODUCER_ENABLE = "bullet.spark.dsl.data.producer.enable"


  val DATA_PRODUCER_PARALLELISM = "bullet.spark.data.producer.parallelism"
  val DATA_PRODUCER_CLASS_NAME = "bullet.spark.data.producer.class.name"
  val CHECKPOINT_DIR = "bullet.spark.checkpoint.dir"
  val RECOVER_FROM_CHECKPOINT_ENABLE = "bullet.spark.recover.from.checkpoint.enable"
  val APP_NAME = "bullet.spark.app.name"
  val LOOP_PUBSUB_OVERRIDES = "bullet.spark.loop.pubsub.overrides"
  val METRICS_ENABLED = "bullet.spark.metrics.enabled"
  val FILTER_PARTITION_PARALLEL_MODE_ENABLED = "bullet.spark.filter.partition.parallel.mode.enabled"
  val FILTER_PARTITION_MODE_PARALLELISM = "bullet.spark.filter.partition.parallel.mode.parallelism"
  val FILTER_PARTITION_PARALLEL_MODE_MIN_QUERY_THRESHOLD =
    "bullet.spark.filter.partition.parallel.mode.min.query.threshold"
  val QUERY_UNION_CHECKPOINT_DURATION_MULTIPLIER = "bullet.spark.query.union.checkpoint.duration.multiplier"
  val JOIN_CHECKPOINT_DURATION_MULTIPLIER = "bullet.spark.join.checkpoint.duration.multiplier"

  val SPARK_STREAMING_CONFIG_PREFIX = "spark."
  val DEFAULT_CONFIGURATION = "bullet_spark_defaults.yaml"

  @volatile private var instance: Broadcast[BulletSparkConfig] = _

  def getInstance(ssc: StreamingContext, config: BulletSparkConfig): Broadcast[BulletSparkConfig] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = ssc.sparkContext.broadcast(config)
        }
      }
    }
    instance
  }

  // Defaults
  private val DEFAULT_QUERY_BLOCK_SIZE = 1
  private val DEFAULT_QUERY_COALESCE_PARTITIONS = 10
  private val DEFAULT_BATCH_DURATION_MS = 1000
  private val DEFAULT_DATA_PRODUCER_PARALLELISM = 1
  private val DEFAULT_CHECKPOINT_DIR = "/tmp/spark-checkpoint"
  private val DEFAULT_RECOVER_FROM_CHECKPOINT_ENABLE = false
  private val DEFAULT_APP_NAME = "BulletSparkStreamingJob"
  private val DEFAULT_LOOP_PUBSUB_OVERRIDES = new HashMap[String, AnyRef]()
  private val DEFAULT_METRICS_ENABLED = false
  private val DEFAULT_FILTER_PARTITION_PARALLEL_MODE_ENABLED = false
  private val DEFAULT_FILTER_PARTITION_MODE_PARALLELISM = 4
  private val DEFAULT_FILTER_PARTITION_PARALLEL_MODE_MIN_QUERY_THRESHOLD = 10
  private val DEFAULT_QUERY_UNION_CHECKPOINT_DURATION_MULTIPLIER = 10
  private val DEFAULT_JOIN_CHECKPOINT_DURATION_MULTIPLIER = 10

  private val VALIDATOR = BulletConfig.getValidator
  VALIDATOR.define(QUERY_BLOCK_SIZE)
           .defaultTo(DEFAULT_QUERY_BLOCK_SIZE)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))
  VALIDATOR.define(QUERY_COALESCE_PARTITIONS)
           .defaultTo(DEFAULT_QUERY_COALESCE_PARTITIONS)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))
  VALIDATOR.define(BATCH_DURATION_MS)
           .defaultTo(DEFAULT_BATCH_DURATION_MS)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))
  VALIDATOR.define(DATA_PRODUCER_PARALLELISM)
           .defaultTo(DEFAULT_DATA_PRODUCER_PARALLELISM)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))
  VALIDATOR.define(CHECKPOINT_DIR)
           .defaultTo(DEFAULT_CHECKPOINT_DIR)
           .checkIf(asJavaPredicate(Validator.isString))
  VALIDATOR.define(RECOVER_FROM_CHECKPOINT_ENABLE)
           .defaultTo(DEFAULT_RECOVER_FROM_CHECKPOINT_ENABLE)
           .checkIf(asJavaPredicate(Validator.isBoolean))
  VALIDATOR.define(APP_NAME)
           .defaultTo(DEFAULT_APP_NAME)
           .checkIf(asJavaPredicate(Validator.isString))
  VALIDATOR.define(LOOP_PUBSUB_OVERRIDES)
           .checkIf(asJavaPredicate(Validator.isMap))
           .defaultTo(DEFAULT_LOOP_PUBSUB_OVERRIDES)
  VALIDATOR.define(METRICS_ENABLED)
           .defaultTo(DEFAULT_METRICS_ENABLED)
           .checkIf(asJavaPredicate(Validator.isBoolean))
  VALIDATOR.define(FILTER_PARTITION_PARALLEL_MODE_ENABLED)
           .defaultTo(DEFAULT_FILTER_PARTITION_PARALLEL_MODE_ENABLED)
           .checkIf(asJavaPredicate(Validator.isBoolean))
  VALIDATOR.define(FILTER_PARTITION_MODE_PARALLELISM)
           .defaultTo(DEFAULT_FILTER_PARTITION_MODE_PARALLELISM)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))
  VALIDATOR.define(FILTER_PARTITION_PARALLEL_MODE_MIN_QUERY_THRESHOLD)
           .defaultTo(DEFAULT_FILTER_PARTITION_PARALLEL_MODE_MIN_QUERY_THRESHOLD)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))
  VALIDATOR.define(QUERY_UNION_CHECKPOINT_DURATION_MULTIPLIER)
           .defaultTo(DEFAULT_QUERY_UNION_CHECKPOINT_DURATION_MULTIPLIER)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))
  VALIDATOR.define(JOIN_CHECKPOINT_DURATION_MULTIPLIER)
           .defaultTo(DEFAULT_JOIN_CHECKPOINT_DURATION_MULTIPLIER)
           .checkIf(asJavaPredicate(Validator.isPositiveInt))
           .castTo(asJavaFunction(Validator.asInt))

  // For testing.
  private[spark] def clearInstance(): Unit = synchronized {
    instance = null
  }
}

/**
 * Stores the Bullet Spark configuration.
 *
 * @constructor Create a BulletSparkConfig instance that merges configs from another Config.
 * @param other The other config to wrap.
 */
class BulletSparkConfig(other: Config)
  extends BulletConfig(BulletSparkConfig.DEFAULT_CONFIGURATION) with BulletSparkLogger {
  // Load Bullet and Spark defaults. Then merge the other.
  merge(other)
  BulletSparkConfig.VALIDATOR.validate(this)
  logger.info("Merged settings:\n {}", this)

  /**
   * @constructor Create a BulletSparkConfig instance that loads the specific file augmented with defaults.
   * @param file YAML file to load.
   */
  def this(file: String) {
    this(new Config(file))
  }
}
