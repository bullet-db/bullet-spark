/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark

import java.util.Collections

import com.yahoo.bullet.common.BulletConfig
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer

class MockDeserializer(val configuration: BulletConfig) extends BulletDeserializer(configuration) {
  override def deserialize(o: Any): AnyRef = {
    Collections.singletonMap("field", "mock_field")
  }
}
