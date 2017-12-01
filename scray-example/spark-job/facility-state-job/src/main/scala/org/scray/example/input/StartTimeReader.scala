package org.scray.example.input

import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.TopicPartition
import scala.annotation.tailrec
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Get value of attribute 'timestamp' in string as long from given kafka stream.
 */
class StartTimeReader(bootstrapServers: String, topic: String) extends LazyLogging {

  val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "FacilityStateJobStartTimeReader_" + System.currentTimeMillis())

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Collections.singletonList(topic))

  @tailrec
  final def getRecordTimestamp: Long = {

    val records = consumer.poll(1).iterator()

    if (records.hasNext) {
      records.next().value().split("timestamp\":")(1).split("}")(0).toLong
    } else {
      logger.info(s"Poll for window start time value. Topic ${consumer.listTopics()}")
      Thread.sleep(500)
      getRecordTimestamp
    }
  }
}