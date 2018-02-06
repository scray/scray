package org.scray.example.input

import org.apache.kafka.common.serialization.StringDeserializer
import scala.io.Source._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import com.fasterxml.jackson.databind.DeserializationFeature
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import collection.JavaConverters._
import java.util.LinkedList
import java.util.ArrayList
import com.fasterxml.jackson.databind.SerializationFeature
import com.typesafe.scalalogging.Logger
import scala.util.Failure
import scala.util.Try

/**
 * Represents highest offsets of all partitions for a given topic
 */
case class KafkaEndPossition(topic: String, partition: Int, offset: Long) { 
  
  @transient
  val logger = Logger("org.scray.example.input.KafkaStartPossition")
  
  def toJsonString: Try[String] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    
    try {
      Try(mapper.writeValueAsString(this))
    } catch {
      case e: Exception => Failure(e) 
    } 
  }

  def fromJsonString(json: String): Option[KafkaEndPossition] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      return Some(mapper.readValue(json, classOf[KafkaEndPossition]))
    } catch {
      case e: Throwable => {
        logger.warn(s"Exception while parsing KafkaStartPossition ${json}. ${e.getMessage}")
        return None
      }
    }
    return None
  }
}

/**
 * Represents lowest offsets of all partitions for a given topic
 */
case class KafkaStartPossition(topic: String, partition: Int, offset: Long) { 
  
  @transient
  val logger = Logger("org.scray.example.input.KafkaStartPossition")
  
  def toJsonString: Try[String] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    
    try {
      Try(mapper.writeValueAsString(this))
    } catch {
      case e: Exception => Failure(e) 
    } 
  }

  def fromJsonString(json: String): Option[KafkaStartPossition] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      return Some(mapper.readValue(json, classOf[KafkaStartPossition]))
    } catch {
      case e: Throwable => {
        logger.warn(s"Exception while parsing KafkaStartPossition ${json}. ${e.getMessage}")
        return None
      }
    }
    return None
  }
}

/**
 * Read latest offset for a given Kafka topic
 */
class KafkaOffsetReader(bootstrasServers: String) {

  def getCurrentKafkaHighestOffsets(topic: String): List[KafkaEndPossition] = {
    val props = new Properties();
    props.put("bootstrap.servers", bootstrasServers);
    props.put("group.id", "KafkaOffsetReader" + System.currentTimeMillis());
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    val consumer = new KafkaConsumer[String, String](props);

    val topics = topic :: Nil
    consumer.subscribe(topics.asJava)
    consumer.poll(100L)

    val topicPartitions = getPartitions(consumer, topic)

    consumer.seekToEnd(topicPartitions)

    val partitionOffsets = topicPartitions.
      asScala.
      map(topicPartition => {
        val offset = consumer.position(topicPartition)

        KafkaEndPossition(
          topicPartition.topic(),
          topicPartition.partition(),
          offset)
      })

    partitionOffsets.toList
  }
  
  def getCurrentKafkaLowestOffsets(topic: String): List[KafkaStartPossition] = {
    val props = new Properties();
    props.put("bootstrap.servers", bootstrasServers);
    props.put("group.id", "KafkaOffsetReader" + System.currentTimeMillis());
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    val consumer = new KafkaConsumer[String, String](props);

    val topics = topic :: Nil
    consumer.subscribe(topics.asJava)
    consumer.poll(100L)

    val topicPartitions = getPartitions(consumer, topic)

    consumer.seekToBeginning(topicPartitions)

    val partitionOffsets = topicPartitions.
      asScala.
      map(topicPartition => {
        val offset = consumer.position(topicPartition)

        KafkaStartPossition(
          topicPartition.topic(),
          topicPartition.partition(),
          offset)
      })

    partitionOffsets.toList
  }

  def getPartitions(consumer: KafkaConsumer[String, String], topic: String): java.util.List[TopicPartition] = {

    val topicPartitions: java.util.List[TopicPartition] = new LinkedList[TopicPartition]()

    consumer.partitionsFor(topic).
      asScala.
      map(partitionInfo =>
        topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition())))

    topicPartitions
  }
}