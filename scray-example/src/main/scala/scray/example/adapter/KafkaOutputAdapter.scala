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


class KafkaOutputAdapter(val inputQueue: BlockingQueue[Share]) extends Thread {
  
  override def run() {
        val rnd = new Random();
 
        val props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "com.seeburger.bdq.spark.serializers.GenericKafkaKryoSerializer");
        props.put("partitioner.class", "scray.example.adapter.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        val config = new ProducerConfig(props);
        val producer = new Producer[String, Share](config)
 
        for(events <- 1 until 100) { 
               val message = new KeyedMessage[String, Share]("test", events.toString(), inputQueue.take());
               println(message)
               producer.send(message);
        }
        producer.close;
    
//    while(true) {
//      println(inputQueue.take())
//    }
  }
}