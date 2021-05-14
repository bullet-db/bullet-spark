/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.util.Collections

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.pubsub.Metadata
import com.yahoo.bullet.query.QueryUtils.{makeCountDistinctQuery, makeFieldFilterQuery, makeSimpleAggregationQuery}
import com.yahoo.bullet.query.Window
import com.yahoo.bullet.querying.{Querier, RateLimitError}
import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.result.RecordBox
import com.yahoo.bullet.spark.data.{BulletData, BulletErrorData, FilterResultData, RunningQueryData}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.rdd.RDD

class FilterStreamingTest extends BulletSparkTest {
  private val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
  private val metadata = new Metadata()

  behavior of "The filter stage"

  it should "output filter results with no results" in {
    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    // Not expired.
    val query = makeFieldFilterQuery("b235gf23b")
    val runningQuery = new NeverExpiringRunningQuery("id", query, metadata)
    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD(Seq(RecordBox.get.add("field", "wontmatch").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.count(_._2.asInstanceOf[FilterResultData].data != null) should equal(0)
    }
  }

  it should "output filter results with one result" in {
    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    // Not expired.
    val query = makeFieldFilterQuery("b235gf23b")
    val runningQuery = new NeverExpiringRunningQuery("id", query, metadata)
    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD(Seq(
      RecordBox.get.add("field", "b235gf23b").getRecord,
      RecordBox.get.add("field", "wontmatch").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(1)
      val querier = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.ALL, broadcastedConfig)
      outputCollector.last.foreach(d => querier.combine(d._2.asInstanceOf[FilterResultData].data))
      querier.getRecords.size() should equal(1)
    }
  }

  it should "output filter results with two results" in {
    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    // Not expired.
    val query = makeFieldFilterQuery("b235gf23b", 4)
    val runningQuery = new NeverExpiringRunningQuery("id", query, metadata)
    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD(Seq(
      RecordBox.get.add("field", "b235gf23b").getRecord,
      RecordBox.get.add("field", "b235gf23b").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(1)
      val querier = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.ALL, broadcastedConfig)
      outputCollector.last.foreach(d => querier.combine(d._2.asInstanceOf[FilterResultData].data))
      querier.getRecords.size() should equal(2)
    }
  }

  it should "output filter results with parallel filtering is enabled" in {
    val configWithParallelEnabled = new BulletSparkConfig("src/test/resources/test_config.yaml")
    configWithParallelEnabled.set(BulletSparkConfig.FILTER_PARTITION_PARALLEL_MODE_ENABLED, true)
    configWithParallelEnabled.set(BulletSparkConfig.FILTER_PARTITION_MODE_PARALLELISM, 2)
    configWithParallelEnabled.set(BulletSparkConfig.FILTER_PARTITION_PARALLEL_MODE_MIN_QUERY_THRESHOLD, 1)

    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, configWithParallelEnabled)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    // Not expired.
    val query = makeFieldFilterQuery("b235gf23b", 4)
    val query1 = makeFieldFilterQuery("b235gf23c", 4)
    val runningQuery = new NeverExpiringRunningQuery("id", query, metadata)
    val runningQuery1 = new NeverExpiringRunningQuery("id1", query1, metadata)
    inputQueries += sc.makeRDD(Seq(
      ("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery)),
      ("id1", new RunningQueryData(metadata = metadata, runningQuery = runningQuery1))))
    inputBulletRecords += sc.makeRDD(Seq(
      RecordBox.get.add("field", "b235gf23b").getRecord,
      RecordBox.get.add("field", "b235gf23b").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(1)
      val querier = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.ALL, broadcastedConfig)
      outputCollector.last.foreach(d => querier.combine(d._2.asInstanceOf[FilterResultData].data))
      querier.getRecords.size() should equal(2)
    }
  }

  it should "output empty results when input query is an expired raw query" in {
    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    val query = makeFieldFilterQuery("b235gf23b")
    val runningQuery = new ExpiredRunningQuery("id", query, metadata)
    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD(Seq(RecordBox.get.add("field", "b235gf23b").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.count(_._2.asInstanceOf[FilterResultData].data != null) should equal(0)
    }
  }

  it should "output empty results when input query is an expired aggregation query" in {
    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]


    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    val query = makeCountDistinctQuery(Collections.singletonList("field"), "COUNT DISTINCT")
    val runningQuery = new ExpiredRunningQuery("id", query, metadata)
    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD(Seq(RecordBox.get.add("field", "b235gf23b").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(1)
      val querier = BulletSparkUtils.createBulletQuerier(runningQuery, Querier.Mode.ALL, broadcastedConfig)
      outputCollector.last.foreach(d => querier.combine(d._2.asInstanceOf[FilterResultData].data))
      querier.getRecords.size() should equal(1)
      querier.getRecords.get(0).typedGet("COUNT DISTINCT").getValue should equal(0)
    }
  }

  it should "output empty results when there is no input query" in {
    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    inputBulletRecords += sc.makeRDD(Seq(RecordBox.get.add("field", "b235gf23b").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.count(_._2.asInstanceOf[FilterResultData].data != null) should equal(0)
    }
  }

  it should "output every record result when input query has a reactive window" in {
    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]


    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    val query = makeSimpleAggregationQuery(1, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1)
    val runningQuery = new NeverExpiringRunningQuery("id", query, metadata)
    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD(Seq(RecordBox.get.add("field", "b235gf23b").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(1)
    }

    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD(Seq(RecordBox.get.add("field", "b235gf23b").getRecord,
      RecordBox.get.add("field", "b235gf23c").getRecord))
    wait1second() // T = 1s

    eventually {
      outputCollector.last.length should equal(2)
    }
  }

  it should "output a rate limit error when exceeding the rate limit" in {
    config.set(BulletConfig.RATE_LIMIT_ENABLE, true)
    config.set(BulletConfig.RATE_LIMIT_TIME_INTERVAL, 1)
    config.set(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, 1)

    val inputQueries: mutable.Queue[RDD[(String, RunningQueryData)]] = mutable.Queue()
    val inputBulletRecords: mutable.Queue[RDD[BulletRecord[_ <: java.io.Serializable]]] = mutable.Queue()
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val inputQueryStream = ssc.queueStream(inputQueries)
    val inputBulletRecordStream = ssc.queueStream(inputBulletRecords)

    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val outputStream = FilterStreaming.filter(inputQueryStream, inputBulletRecordStream, broadcastedConfig)

    outputStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.start()

    // Not expired.
    val query = makeSimpleAggregationQuery(1, Window.Unit.RECORD, 1, Window.Unit.RECORD, 1)
    val runningQuery = new NeverExpiringRunningQuery("id", query, metadata)
    inputQueries += sc.makeRDD(Seq(("id", new RunningQueryData(metadata = metadata, runningQuery = runningQuery))))
    inputBulletRecords += sc.makeRDD((1 to 1000).map(_ => RecordBox.get.add("field", "b235gf23b").getRecord))
    wait1second() // T = 1s

    eventually {
      val errors = outputCollector.last.filter(_._2.isInstanceOf[BulletErrorData])
      errors.length should equal(1)
      errors.head._2.asInstanceOf[BulletErrorData].errors.count(
        _.isInstanceOf[RateLimitError]) should equal(1)
    }
  }
}
