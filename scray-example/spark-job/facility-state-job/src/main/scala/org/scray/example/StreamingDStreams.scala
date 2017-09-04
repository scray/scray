package org.scray.example

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.mapreduce.InputFormat
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.storage.StorageLevel
import kafka.serializer.Decoder
import kafka.serializer.StringDecoder
import org.scray.example.serializers.GenericKafkaKryoSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamingDStreams {
  
  val KAFKA_CONSUMER_GROUP = "group1"
  
  /**
   * initializes distributed stream source for a generic Kafka source, which needs serializers to be specified
   */
  def getKafkaStreamSource[K, V, U <: Decoder[_], T <: Decoder[_]](
      ssc: StreamingContext, 
      kafkaDStreamURL: Option[String], 
      kafkaTopics: Option[String], 
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
      (implicit k: ClassTag[K], v: ClassTag[V], u: ClassTag[U], t: ClassTag[T]): Option[InputDStream[ConsumerRecord[K, V]]] = {
    
    val kafkaTopicsf = new Array[String](1)
    kafkaTopicsf(0) = kafkaTopics.get
    
    val kafkaParams = Map[String, String](
        "bootstrap.servers" -> kafkaDStreamURL.get,
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "group.id" -> KAFKA_CONSUMER_GROUP)

    Some(KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[K, V](kafkaTopicsf, kafkaParams)))
  }
  
  /**
   * initializes distributed stream source for a (String, String) Kafka source
   */
  def getKafkaStringSource(
      ssc: StreamingContext, 
      kafkaDStreamURL: Option[String], 
      kafkaTopic: Option[String], 
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
      ):Option[InputDStream[ConsumerRecord[String, String]]] = 
        
    getKafkaStreamSource[String, String, StringDecoder, StringDecoder](ssc, kafkaDStreamURL, kafkaTopic, storageLevel)
  

          
  /**
   * initializes distributed stream source for hdfs text files
   */
  def getTextStreamSource(ssc: StreamingContext, 
      hdfsDStreamURL: Option[String]): Option[DStream[String]] = hdfsDStreamURL.map(url => ssc.textFileStream(url))

}