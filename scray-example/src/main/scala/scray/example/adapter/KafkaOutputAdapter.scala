package scray.example.adapter

import java.util.concurrent.BlockingQueue
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import java.util.Properties
import scala.util.Random
import java.util.Date
import kafka.utils.VerifiableProperties
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import com.seeburger.bdq.spark.serializers.GenericKafkaKryoSerializer
import com.typesafe.scalalogging.slf4j.LazyLogging


class KafkaOutputAdapter(val inputQueue: BlockingQueue[Share]) extends Thread with LazyLogging {
  
  override def run() {
 
        val props = new Properties();
        props.put("metadata.broker.list", "localhost:4242");
        props.put("serializer.class", "com.seeburger.bdq.spark.serializers.GenericKafkaKryoSerializer");
        props.put("partitioner.class", "scray.example.adapter.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        val config = new ProducerConfig(props);
        val producer = new Producer[String, Share](config)
 
        while(true) {
          val message = new KeyedMessage[String, Share]("test", "1", inputQueue.take());
          logger.debug(s"Send message ${message}")
          println(message)
          producer.send(message)
        }
        producer.close
  }
}