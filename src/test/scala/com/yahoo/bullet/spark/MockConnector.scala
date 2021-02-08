package com.yahoo.bullet.spark

// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.dsl.connector.BulletConnector

import java.util

class MockConnector(val configuration: BulletConfig) extends BulletConnector(configuration) {

  override def initialize(): Unit = {
    val shouldThrow = config.get("shouldThrow").asInstanceOf[Boolean]
    if (shouldThrow) {
      throw new RuntimeException("throwing mock exception")
    }
  }

  override def read(): util.List[Object] = {
    var map = new util.HashMap[String, Object]()
    map.put("count", Int.box(1))
    map.put("field", "fake_field")
    var list = new util.ArrayList[Object]()
    list.add(map)
    list
  }

  override def close(): Unit = {}
}