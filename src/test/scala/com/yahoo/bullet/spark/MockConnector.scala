package com.yahoo.bullet.spark

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.dsl.connector.BulletConnector
import java.util

import scala.collection.mutable.ListBuffer

object MockDataSource {
  var data: ListBuffer[AnyRef] = ListBuffer.empty[AnyRef]

  def pop(): AnyRef = {
    if (data.isEmpty) {
      null
    } else {
      data.remove(0)
    }
  }
}

class MockConnector(val configuration: BulletConfig) extends BulletConnector(configuration) {
  override def initialize(): Unit = {}

  override def read(): util.List[Object] = {
    val list = new util.ArrayList[Object]()
    val x = MockDataSource.pop()
    if (x != null) {
      list.add(x)
    }
    list
  }

  override def close(): Unit = {}
}
