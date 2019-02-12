package com.yahoo.bullet.spark
import com.yahoo.bullet.dsl.BulletDSLConfig
import com.yahoo.bullet.dsl.converter.BulletRecordConverter
import com.yahoo.bullet.dsl.deserializer.BulletDeserializer
import com.yahoo.bullet.record.BulletRecord
import com.yahoo.bullet.spark.utils.BulletSparkConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class DSLDataProducer extends DataProducer {
  private var converter: BulletRecordConverter = _
  private var deserializer: BulletDeserializer = _

  /**
    * Get Bullet record stream from users' source of data.
    *
    * In this method, any transformations to the users' data can be done.
    *
    * @param ssc    The StreamingContext that can be used to define an arbitrary DAG to compute the BulletRecord stream.
    * @param bulletSparkConfig The [[com.yahoo.bullet.spark.utils.BulletSparkConfig]] containing all the settings.
    * @return The BulletRecord stream, which will be used as the input to Bullet.
    */
  override def getBulletRecordStream(ssc: StreamingContext, bulletSparkConfig: BulletSparkConfig): DStream[BulletRecord] = {
    val config = new BulletDSLConfig(bulletSparkConfig)
    val receiver = new DSLReceiver(config)
    val converter = BulletRecordConverter.from(config)
    val deserializer = BulletDeserializer.from(config)
    val objectStream = ssc.receiverStream(receiver).asInstanceOf[DStream[AnyRef]]
    
    objectStream.map(deserializer.deserialize).map(converter.convert)
  }
}
