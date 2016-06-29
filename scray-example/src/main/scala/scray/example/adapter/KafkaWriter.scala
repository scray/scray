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
import scala.annotation.tailrec


class KafkaWriter(val inputQueue: BlockingQueue[Share]) extends Thread with LazyLogging {
  
  override def run() {

    @tailrec def unendingStream(a: => Stream[(Int, Long)], count: Int): Stream[(Int, Long)] = {
      unendingStream((count + 1, if(count % 1000 == 0) System.currentTimeMillis() else a.head._2) #:: a, count + 1)
    }
    
        val props = new Properties();
        props.put("metadata.broker.list", "10.11.22.37:9092");
        props.put("serializer.class", "com.seeburger.bdq.spark.serializers.GenericKafkaKryoSerializer");
        props.put("partitioner.class", "scray.example.adapter.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        val config = new ProducerConfig(props);
        val producer = new Producer[String, Share](config)
        var secondDiff = 0L

        unendingStream(Stream.empty[(Int, Long)], 0).par.foreach { number => 
          val message = new KeyedMessage[String, Share]("test2", "1", inputQueue.take());
          producer.send(message)
          if((number._1 % 1000) == 0) {
            println(s"Send message ${message} Diff ${System.currentTimeMillis() - number._2}")
          }
        }
        
        
//        filter { _._1 % 1000 == 0 }.foreach { x => ??? }
//        
//        
//        while(true) {
//          val message = new KeyedMessage[String, Share]("test2", "1", inputQueue.take());
//          
//          if((count % 1000) == 0) {
//            
//            secondDiff = System.currentTimeMillis()
//          }
//          count = count + 1
//          producer.send(message)
//        }
        producer.close
  }
}