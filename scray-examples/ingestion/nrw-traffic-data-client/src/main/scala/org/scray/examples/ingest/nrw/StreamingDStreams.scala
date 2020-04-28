package org.scray.examples.ingest.nrw

import scala.collection.mutable.HashMap

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.typesafe.scalalogging.LazyLogging


object StreamingDStreams extends LazyLogging {
  
  val KAFKA_CONSUMER_GROUP = "nrw-traffic-data-client"
  
  /**
   * initializes distributed stream source for a (String, String) Kafka source
   */
   def getKafkaStringSource(
     ssc: StreamingContext,
     bootstrapServer: Option[String],
     kafkaTopics: Array[String],
     storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
     additionalKafkaParameters: scala.collection.mutable.Map[String, Object] = new HashMap[String, Object]()): Option[DStream[String]] = {
  
     // Set default values
     additionalKafkaParameters.put("bootstrap.servers",
       bootstrapServer.getOrElse({
         logger.warn("No bootstrap server defined. Use 127.0.0.1")
         "127.0.0.1:9092"
       })
     )
  
     additionalKafkaParameters.get("key.deserializer").getOrElse(
       additionalKafkaParameters.put("key.deserializer", classOf[StringDeserializer])
     )
  
     additionalKafkaParameters.get("value.deserializer").getOrElse(
       additionalKafkaParameters.put("value.deserializer", classOf[StringDeserializer])
     )
     
     additionalKafkaParameters.get("group.id").getOrElse(
       additionalKafkaParameters.put("group.id", KAFKA_CONSUMER_GROUP)
     )
  
     val stream = KafkaUtils.createDirectStream[String, String](
       ssc,
       PreferConsistent,
       Subscribe[String, String](kafkaTopics, additionalKafkaParameters)).map(_.value()
     )

    Some(stream)
  }
          
  /**
   * initializes distributed stream source for hdfs text files
   */
  def getTextStreamSource(ssc: StreamingContext, 
      hdfsDStreamURL: Option[String]): Option[DStream[String]] = hdfsDStreamURL.map(url => ssc.textFileStream(url))

}