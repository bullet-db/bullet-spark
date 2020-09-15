/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import scala.collection.mutable

import com.yahoo.bullet.common.BulletError
import com.yahoo.bullet.pubsub.Metadata
import com.yahoo.bullet.pubsub.Metadata.Signal
import com.yahoo.bullet.query.QueryUtils.makeFieldFilterQuery
import com.yahoo.bullet.querying.{Querier, RunningQuery}
import com.yahoo.bullet.result.RecordBox
import com.yahoo.bullet.spark.data.{BulletErrorData, BulletResult, QuerierData}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.rdd.RDD

class ResultEmitterTest extends BulletSparkTest {
  behavior of "The result emitter"

  it should "emit nothing when failing to create a pubsub" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    config.set("bullet.pubsub.class.name", "fake.class")
    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val inputQueries: mutable.Queue[RDD[(String, BulletResult)]] = mutable.Queue()
    val inputQueryStream = ssc.queueStream(inputQueries)

    CustomPublisher.publisher.reset()

    ResultEmitter.emit(inputQueryStream, ResultPublisher.getInstance(ssc, broadcastedConfig))

    ssc.start()

    val result =new BulletResult()
    result.addFeedback(BulletSparkUtils.createFeedbackMessage("id", Signal.KILL))
    inputQueries += sc.makeRDD(Seq(("id", result)))
    wait1second()

    eventually {
      CustomPublisher.publisher.sent.length should equal(0)
    }
  }

  it should "emit nothing when publishing null" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val inputQueries: mutable.Queue[RDD[(String, BulletResult)]] = mutable.Queue()
    val inputQueryStream = ssc.queueStream(inputQueries)

    ResultEmitter.emit(inputQueryStream, ResultPublisher.getInstance(ssc, broadcastedConfig))

    ssc.start()

    val result =new BulletResult()
    result.add(null)
    result.addFeedback(null)
    inputQueries += sc.makeRDD(Seq(("id", result)))
    wait1second()

    eventually {
      CustomPublisher.publisher.sent.length should equal(0)
    }
  }

  it should "emit nothing when the stream is empty" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val inputQueries: mutable.Queue[RDD[(String, BulletResult)]] = mutable.Queue()
    val inputQueryStream = ssc.queueStream(inputQueries)

    ResultEmitter.emit(inputQueryStream, ResultPublisher.getInstance(ssc, broadcastedConfig))

    ssc.start()
    wait1second()

    eventually {
      CustomPublisher.publisher.sent.length should equal(0)
    }
  }

  it should "emit all results based on the type" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val inputQueries: mutable.Queue[RDD[(String, BulletResult)]] = mutable.Queue()
    val query = makeFieldFilterQuery("b235gf23b", 5)
    val runningQuery = new RunningQuery("id1", query, new Metadata())

    val inputQueryStream = ssc.queueStream(inputQueries)

    ResultEmitter.emit(inputQueryStream, ResultPublisher.getInstance(ssc, broadcastedConfig))

    ssc.start()

    val bulletQuery = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    val result1 = new BulletResult()
    result1.add(
      BulletSparkUtils.createResultMessage("id1", new QuerierData(null, bulletQuery, null)))
    val result2 =new BulletResult()
    result2.add(BulletSparkUtils.createErrorMessage(
      "id2", new BulletErrorData(null, BulletError.makeError("error", ""))))
    result2.addFeedback(BulletSparkUtils.createFeedbackMessage("id2", Signal.KILL))
    inputQueries += sc.makeRDD(Seq(("id1", result1), ("id2", result2)))
    wait1second() // T = 1s

    eventually {
      CustomPublisher.publisher.sent.length should equal(2)
      CustomPublisher.feedbackPublisher.sent.length should equal(1)
    }
  }
}
