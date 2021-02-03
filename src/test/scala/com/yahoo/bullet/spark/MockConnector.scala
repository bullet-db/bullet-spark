package com.yahoo.bullet.spark

// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.dsl.connector.BulletConnector

import java.util

class MockConnector(val configuration: BulletConfig) extends BulletConnector(configuration) {

  override def initialize(): Unit = {
    val shouldThrow = config.get("DSLReceiverTest").asInstanceOf[Boolean]
    if (shouldThrow) {
      throw new RuntimeException("throwing mock exception")
    }
  }

  override def read(): util.List[AnyRef] = {
    List(Map("count" -> 1, "field" -> "fake_field").asJava).asJava.asInstanceOf[util.List[AnyRef]]
  }

  override def close(): Unit = {}
}
