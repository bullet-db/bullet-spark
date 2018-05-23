/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.utils.BulletSparkConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object DataProducer {
  /**
   * Create a DataProducer instance using the class specified in the config file.
   *
   * @param config The [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] containing the class name and PubSub settings.
   * @return An instance of the specified class initialized with settings from the input file and defaults.
   * @throws RuntimeException if anything went wrong.
   */
  @throws(classOf[RuntimeException])
  def getProducer(config: BulletSparkConfig): DataProducer = {
    try {
      val dataGeneratorClassName = config.get(BulletSparkConfig.DATA_PRODUCER_CLASS_NAME).asInstanceOf[String]
      val clazz = Class.forName(dataGeneratorClassName)
      val constructor = clazz.getDeclaredConstructor()
      constructor.newInstance().asInstanceOf[DataProducer]
    } catch {
      case e: Exception => throw new RuntimeException("Can not create BulletRecordProducer instance.", e)
    }
  }
}

/**
 * Trait that can be implemented to produce BulletRecord data as input to Bullet running on Spark Streaming.
 *
 * This trait is used to plugin users' source of data to Bullet. The data can be from anywhere. For example, Kafka,
 * HDFS, H3, Kinesis etc. This generally involves hooking in a Receiver. Users can also do any transformations on the
 * data before emitting it to Bullet in their own implementations by using the provided Spark Streaming Context. This
 * will be the same context used to wire in the rest of the Bullet DAG.
 */
trait DataProducer {
  /**
   * Get Bullet record stream from users' source of data.
   *
   * In this method, any transformations to the users' data can be done.
   *
   * @param ssc    The StreamingContext that can be used to define an arbitrary DAG to compute the BulletRecord stream.
   * @param config The [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] containing all the settings.
   * @return The BulletRecord stream, which will be used as the input to Bullet.
   */
  def getBulletRecordStream(ssc: StreamingContext, config: BulletSparkConfig): DStream[BulletRecord]
}
