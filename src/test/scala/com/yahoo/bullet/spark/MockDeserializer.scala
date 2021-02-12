package com.yahoo.bullet.spark

import java.util.Collections

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer

class MockDeserializer(val configuration: BulletConfig) extends BulletDeserializer(configuration) {
  override def deserialize(o: Any): AnyRef = {
    Collections.singletonMap("field", "mock_field")
  }
}
