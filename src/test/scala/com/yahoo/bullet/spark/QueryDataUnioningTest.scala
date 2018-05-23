/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.yahoo.bullet.parsing.Aggregation.Type.RAW
import com.yahoo.bullet.parsing.QueryUtils.{makeAggregationQuery, makeFieldFilterQuery}
import com.yahoo.bullet.parsing.Window
import com.yahoo.bullet.pubsub.Metadata.Signal
import com.yahoo.bullet.pubsub.PubSubMessage
import com.yahoo.bullet.spark.data.{BulletData, BulletErrorData, BulletSignalData, RunningQueryData}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.rdd.RDD

class QueryDataUnioningTest extends BulletSparkTest {
  behavior of "The query data unioning stage"

  it should "output all valid queries" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val inputQueries: mutable.Queue[RDD[PubSubMessage]] = mutable.Queue()

    val inputQueryStream = ssc.queueStream(inputQueries)
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val queryStream = QueryDataUnioning.getAllValidQueries(ssc, inputQueryStream, broadcastedConfig)
    queryStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")
    ssc.start()

    val pubSubMessage1 = new PubSubMessage("id1", makeFieldFilterQuery("b235gf23b"))
    // Json parsing error.
    val pubSubMessage2 = new PubSubMessage("id2", "This is a json parsing error pubsub message.")
    // Window initialization error.
    val pubSubMessage3 = new PubSubMessage("id3", makeAggregationQuery(
      RAW, null, Window.Unit.RECORD, 10, Window.Unit.RECORD, 10))
    inputQueries += sc.makeRDD(Seq(pubSubMessage1, pubSubMessage2, pubSubMessage3))
    wait1second() // T = 1s

    eventually {
      val list = outputCollector.last.toList
      list.length should equal(3)
      list.count(_._2.isInstanceOf[RunningQueryData]) should equal(1)
      list.count(_._2.isInstanceOf[BulletErrorData]) should equal(2)
    }

    wait1second() // T = 2s

    eventually {
      val list = outputCollector.last.toList
      list.length should equal(1)
      list.head._2.isInstanceOf[RunningQueryData] should equal(true)
    }

    val pubSubMessage4 = BulletSparkUtils.createFeedbackMessage("id1", Signal.KILL)
    inputQueries += sc.makeRDD(Seq(pubSubMessage4))
    wait1second() // T = 3s

    eventually {
      val list = outputCollector.last.toList
      list.length should equal(1)
      list.head._2.isInstanceOf[BulletSignalData] should equal(true)
    }

    wait1second() // T = 4s

    eventually {
      val list = outputCollector.last.toList
      list.length should equal(0)
    }
  }

  it should "output a complete signal when query is timed out" in {
    val config = new BulletSparkConfig("src/test/resources/test_config.yaml")
    val broadcastedConfig = BulletSparkConfig.getInstance(ssc, config)
    val inputQueries: mutable.Queue[RDD[PubSubMessage]] = mutable.Queue()

    val inputQueryStream = ssc.queueStream(inputQueries)
    val outputCollector = ListBuffer.empty[Array[(String, BulletData)]]

    val queryStream = QueryDataUnioning.getAllValidQueries(ssc, inputQueryStream, broadcastedConfig)
    queryStream.foreachRDD(rdd => outputCollector += rdd.collect())

    ssc.checkpoint("target/spark-test")
    ssc.start()

    val pubSubMessage1 = new PubSubMessage("id1", "{\"duration\" = 1}")
    inputQueries += sc.makeRDD(Seq(pubSubMessage1))
    wait1second() // T = 1s

    eventually {
      val list = outputCollector.last.toList
      list.length should equal(1)
      list.count(_._2.isInstanceOf[RunningQueryData]) should equal(1)
    }

    // Sleep 1ms to make the query timeout.
    Thread.sleep(1)

    wait1second() // T = 2s

    eventually {
      val list = outputCollector.last.toList
      list.length should equal(1)
      list.head._2.isInstanceOf[BulletSignalData] should equal(true)
    }
  }
}
