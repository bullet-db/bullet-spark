package com.yahoo.bullet.spark

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.dsl.connector.BulletConnector
import java.util.Collections
import java.util.List

import scala.collection.mutable.ListBuffer

object MockConnector {
  var data: ListBuffer[List[AnyRef]] = ListBuffer.empty[List[AnyRef]]
  var closed = false
  var closeCalled = false

  def pop(): List[AnyRef] = {
    if (data.isEmpty) {
      Collections.emptyList[AnyRef]
    } else {
      data.remove(0)
    }
  }
}

class MockConnector(val configuration: BulletConfig) extends BulletConnector(configuration) {
  override def initialize(): Unit = {
    MockConnector.closed = false
  }

  override def read(): List[Object] = {
    if (MockConnector.closed) {
      throw new RuntimeException("connector closed")
    }
    MockConnector.pop()
  }

  override def close(): Unit = {
    MockConnector.closed = true
    MockConnector.closeCalled = true
  }
}
