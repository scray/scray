package org.scray.example.input

import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import collection.JavaConverters._
import java.util.LinkedList
import java.util.ArrayList

case class KafkaStartPossition(topic: String, partition: Int, offset: Long)

/**
 * Read latest offset for a given Kafka topic
 */
class KafkaOffsetReader(bootstrasServers: String) {
  
  def getCurrentKafkaOffsets(topic: String) = {
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
    map ( topicPartition => {
      val offset = consumer.position(topicPartition) 
      
      KafkaStartPossition(
          topicPartition.topic(),
          topicPartition.partition(),
          offset)
      }
    )

    partitionOffsets
  }
  
  def getPartitions(consumer: KafkaConsumer[String, String], topic: String): java.util.List[TopicPartition] = {

    val topicPartitions: java.util.List[TopicPartition] = new LinkedList[TopicPartition]()

    consumer.partitionsFor(topic).
    asScala.
    map(partitionInfo => 
      topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
      )

      topicPartitions
  }
}