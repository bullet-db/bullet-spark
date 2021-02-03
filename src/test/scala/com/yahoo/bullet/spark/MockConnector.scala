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
    var map = new util.HashMap[String, Object]()
    //map.put("count", 1L)
    map.put("field", "fake_field")
    var list = new util.ArrayList[Object]()
    list.add(map)
    list
    //List(Map("count" -> 1, "field" -> "fake_field").asJava).asJava.asInstanceOf[util.List[AnyRef]]
  }

  override def close(): Unit = {}
}
