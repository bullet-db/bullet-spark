/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import com.yahoo.bullet.common.SerializerDeserializer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.yahoo.bullet.pubsub.Metadata.Signal
import com.yahoo.bullet.pubsub.{Metadata, PubSubMessage}
import com.yahoo.bullet.query.{Projection, Query, Window}
import com.yahoo.bullet.query.QueryUtils.makeFieldFilterQuery
import com.yahoo.bullet.query.aggregations.Raw
import com.yahoo.bullet.spark.data.{BulletData, BulletErrorData, BulletSignalData, RunningQueryData}
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkUtils}
import org.apache.spark.rdd.RDD

class QueryDataUnioningTest extends BulletSparkTest {
  private val metadata = new Metadata()

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

    val query = makeFieldFilterQuery("b235gf23b")
    val pubSubMessage1 = new PubSubMessage("id1", SerializerDeserializer.toBytes(query), metadata)
    // Json parsing error.
    val pubSubMessage2 = new PubSubMessage("id2", "This is a json parsing error pubsub message.")
    inputQueries += sc.makeRDD(Seq(pubSubMessage1, pubSubMessage2))
    wait1second() // T = 1s

    eventually {
      val list = outputCollector.last.toList
      list.length should equal(2)
      list.count(_._2.isInstanceOf[RunningQueryData]) should equal(1)
      list.count(_._2.isInstanceOf[BulletErrorData]) should equal(1)
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

    val query = new Query(new Projection(), null, new Raw(1), null, new Window(), 1L)
    val pubSubMessage1 = new PubSubMessage("id1", SerializerDeserializer.toBytes(query), metadata)
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
