package com.yahoo.bullet.spark

import java.util

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer

class MockDeserializer(val configuration: BulletConfig) extends BulletDeserializer(configuration) {
  override def deserialize(obj: Any): AnyRef = {
    val map = new util.HashMap[String, Object]()
    map.put("hello", "world")
    map
  }
}
