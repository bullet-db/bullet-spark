/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.parsing.Aggregation.Type.RAW
import com.yahoo.bullet.parsing.Clause.Operation
import com.yahoo.bullet.parsing.QueryUtils.{makeAggregationQuery, makeSimpleAggregationFilterQuery}
import com.yahoo.bullet.parsing.Window
import com.yahoo.bullet.pubsub.Metadata.Signal
import com.yahoo.bullet.pubsub.{Metadata, PubSubMessage}
import com.yahoo.bullet.querying.{Querier, RateLimitError, RunningQuery}
import com.yahoo.bullet.result.RecordBox
import com.yahoo.bullet.spark.data.{
  BulletData, BulletErrorData, BulletResult, BulletResultType, BulletSignalData, FilterResultData
}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.rdd.RDD

class JoinStreamingTest extends BulletSparkTest {
  private val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
  private val metadata = new Metadata()

  behavior of "The join stage"

  it should "output join results with enough input bullet records" in {
    val inputQueries: mutable.Queue[RDD[(String, BulletData)]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletResult)]]

    val pubSubMessage = new PubSubMessage("id",
      makeSimpleAggregationFilterQuery("field", List("b235gf23b").asJava, Operation.EQUALS, RAW, 2))
    val runningQuery = new RunningQuery("id", pubSubMessage.getContent, config)

    val inputQueryStream = ssc.queueStream(inputQueries)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = JoinStreaming.join(ssc, inputQueryStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")

    ssc.start()

    val bulletQuery1 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery1.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    inputQueries += sc.makeRDD(Seq(("id", new FilterResultData(metadata, runningQuery, bulletQuery1.getData))))

    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(0)
    }

    val bulletQuery2 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery2.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    inputQueries += sc.makeRDD(Seq(("id", new FilterResultData(metadata, runningQuery, bulletQuery2.getData))))


    wait1second() // T = 2s

    eventually {
      outputCollector.last.length should equal(1)
    }
  }

  it should "output join results on query timeout" in {
    val inputQueries: mutable.Queue[RDD[(String, BulletData)]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletResult)]]

    val pubSubMessage = new PubSubMessage("id",
      makeSimpleAggregationFilterQuery("field", List("b235gf23b").asJava, Operation.EQUALS, RAW, 3))
    val runningQuery = new ExpiredRunningQuery("id", pubSubMessage.getContent, config)

    val inputQueryStream = ssc.queueStream(inputQueries)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = JoinStreaming.join(ssc, inputQueryStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")

    ssc.start()

    val bulletQuery = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    inputQueries += sc.makeRDD(Seq(("id", new FilterResultData(metadata, runningQuery, bulletQuery.getData))))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(1)
    }
  }

  it should "output join results when receiving kill or complete signals" in {
    val inputQueries: mutable.Queue[RDD[(String, BulletData)]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletResult)]]

    val pubSubMessage = new PubSubMessage("id",
      makeSimpleAggregationFilterQuery("field", List("b235gf23b").asJava, Operation.EQUALS, RAW, 2))
    val runningQuery = new RunningQuery("id", pubSubMessage.getContent, config)

    val inputQueryStream = ssc.queueStream(inputQueries)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = JoinStreaming.join(ssc, inputQueryStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")

    ssc.start()

    val bulletQuery1 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery1.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    inputQueries += sc.makeRDD(Seq(("id", new FilterResultData(metadata, runningQuery, bulletQuery1.getData))))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(0)
    }

    val signalData = new BulletSignalData(null, Metadata.Signal.KILL)
    inputQueries += sc.makeRDD(Seq(("id", signalData)))

    wait1second() // T = 2s

    eventually {
      outputCollector.last.length should equal(1)
    }
  }

  it should "output join results when receiving rate limited queries" in {
    val inputQueries: mutable.Queue[RDD[(String, BulletData)]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletResult)]]

    val pubSubMessage = new PubSubMessage("id",
      makeSimpleAggregationFilterQuery("field", List("b235gf23b").asJava, Operation.EQUALS, RAW, 2))
    val runningQuery = new RunningQuery("id", pubSubMessage.getContent, config)

    val inputQueryStream = ssc.queueStream(inputQueries)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = JoinStreaming.join(ssc, inputQueryStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")

    ssc.start()

    val bulletQuery1 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery1.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    inputQueries += sc.makeRDD(Seq(("id", new FilterResultData(null, runningQuery, bulletQuery1.getData))))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(0)
    }

    val error = new RateLimitError(0.0, config)
    inputQueries += sc.makeRDD(Seq(("id", new BulletErrorData(metadata = metadata, errors = List(error)))))

    wait1second() // T = 2s

    eventually {
      val errors = outputCollector.last.filter(_._2.results.count(_._2.equals(BulletResultType.FEEDBACK)) > 0)
      errors.length should equal(1)
      val message = errors.head._2.results.head
      val feedback = errors.head._2.results.last
      message._1.getMetadata.getSignal should equal(Signal.FAIL)
      feedback._1.getMetadata.getSignal should equal(Signal.KILL)
    }

    // Receiving a KILL signal after.
    val signalData1 = new BulletSignalData(null, Metadata.Signal.KILL)
    inputQueries += sc.makeRDD(Seq(("id", signalData1)))
    wait1second() // T = 3s

    eventually {
      outputCollector.last.length should equal(0)
    }

    // Receiving a KILL signal after.
    val signalData2 = new BulletSignalData(null, Metadata.Signal.KILL)
    inputQueries += sc.makeRDD(Seq(("id", signalData2)))
    wait1second() // T = 3s

    eventually {
      outputCollector.last.length should equal(0)
    }
  }

  it should "output error results when receiving error input data" in {
    val inputQueries: mutable.Queue[RDD[(String, BulletData)]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletResult)]]

    val pubSubMessage = new PubSubMessage("id", "This is a json format error message.")

    val inputQueryStream = ssc.queueStream(inputQueries)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = JoinStreaming.join(ssc, inputQueryStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")

    ssc.start()

    inputQueries += sc.makeRDD(Seq(("id", BulletSparkUtils.createBulletData(pubSubMessage, broadcastedConfig))))
    wait1second() // T = 1s

    eventually {
      val errors = outputCollector.last.filter(_._2.results.count(_._2.equals(BulletResultType.FEEDBACK)) > 0)
      errors.length should equal(1)
      val feedback = errors.head._2.results.last
      feedback._1.getMetadata.getSignal should equal(Signal.KILL)
    }
  }

  it should "output join results when input query has a reactive window" in {
    val inputQueries: mutable.Queue[RDD[(String, BulletData)]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletResult)]]

    val pubSubMessage = new PubSubMessage("id",
      makeAggregationQuery(RAW, null, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1))
    val runningQuery = new RunningQuery("id", pubSubMessage.getContent, config)

    val inputQueryStream = ssc.queueStream(inputQueries)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = JoinStreaming.join(ssc, inputQueryStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")

    ssc.start()

    val bulletQuery1 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    inputQueries += sc.makeRDD(Seq(("id", new FilterResultData(metadata, runningQuery, bulletQuery1.getData))))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(0)
    }

    val bulletQuery2 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery2.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    val bulletQuery3 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery3.consume(RecordBox.get.add("field", "b235gf23c").getRecord)
    inputQueries += sc.makeRDD(Seq(
      ("id", new FilterResultData(metadata, runningQuery, bulletQuery2.getData)),
      ("id", new FilterResultData(metadata, runningQuery, bulletQuery3.getData))))
    wait1second() // T = 2s

    eventually {
      outputCollector.last.length should equal(2)
    }
  }

  it should "output a rate limit error result when exceeding the rate limit" in {
    config.set(BulletConfig.RATE_LIMIT_ENABLE, true)
    config.set(BulletConfig.RATE_LIMIT_TIME_INTERVAL, 1)
    config.set(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, 1)

    val inputQueries: mutable.Queue[RDD[(String, BulletData)]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletResult)]]

    val pubSubMessage = new PubSubMessage("id",
      makeAggregationQuery(RAW, null, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1))
    val runningQuery = new RunningQuery("id", pubSubMessage.getContent, config)

    val inputQueryStream = ssc.queueStream(inputQueries)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = JoinStreaming.join(ssc, inputQueryStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")

    ssc.start()

    inputQueries += sc.makeRDD(
      (1 to 1000).map(_ => {
        val bulletQuery = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
        bulletQuery.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
        ("id", new FilterResultData(metadata, runningQuery, bulletQuery.getData))
      }))

    wait1second() // T = 1s

    eventually {
      val errors = outputCollector.last.filter(_._2.results.count(_._2.equals(BulletResultType.FEEDBACK)) > 0)
      errors.length should equal(1)
      val results = errors.head._2.results
      val message = results(results.length - 2) // Get the second one from last.
      val feedback = results.last
      message._1.getMetadata.getSignal should equal(Signal.FAIL)
      feedback._1.getMetadata.getSignal should equal(Signal.KILL)
    }

    // No result emitted after errors happened.
    val bulletQuery2 = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.PARTITION, broadcastedConfig)
    bulletQuery2.consume(RecordBox.get.add("field", "b235gf23b").getRecord)
    inputQueries += sc.makeRDD(Seq(("id", new FilterResultData(metadata, runningQuery, bulletQuery2.getData))))
    wait1second() // T = 2s

    eventually {
      outputCollector.last.length should equal(0)
    }
  }
}
