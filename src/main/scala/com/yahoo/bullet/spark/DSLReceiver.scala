package com.yahoo.bullet.spark

import com.yahoo.bullet.dsl.BulletDSLConfig
import com.yahoo.bullet.dsl.connector.BulletConnector
import com.yahoo.bullet.spark.utils.{BulletSparkConfig, BulletSparkLogger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Constructor that does something.
  *
  * @param config The { @link BulletDSLConfig} to load settings from.
  */
class DSLReceiver(val config: BulletDSLConfig) extends Receiver[AnyRef](StorageLevel.MEMORY_AND_DISK_SER) with BulletSparkLogger {
  private var connector: BulletConnector = _

  override def onStart(): Unit = {
    connector = BulletConnector.from(config)
    connector.initialize()
    new Thread() {
      override def run(): Unit = {
        receive()
      }
    }.start()
    logger.info("DSL receiver started.")
  }

  override def onStop(): Unit = {
    if (connector != null) {
      connector.close()
    }
    connector = null
    logger.info("DSL receiver stopped.")
  }

  private def receive(): Unit = {
    while (!isStopped) {
      try {
        val objects = connector.read()
        if (!objects.isEmpty) {
          store(objects.listIterator())
        }
      } catch {
        case t: Throwable =>
          if (connector != null) {
            connector.close()
          }
          restart("Error receiving data.", t)
      }
    }
  }
}
