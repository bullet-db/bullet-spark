/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.spark.utils

import java.io.Serializable

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.yahoo.bullet.record.BulletRecord
import org.apache.spark.serializer.KryoRegistrator

class BulletKryoRegistrator extends KryoRegistrator with Serializable {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[BulletRecord], new JavaSerializer())
  }
}
