package scray.example.adapter

import java.util.Properties

import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import kafka.server.KafkaConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import com.typesafe.scalalogging.slf4j.LazyLogging

class StartKafkaServer extends LazyLogging {
  
      val zooKeeper = new ZooKeeperServer 
      val props = new Properties()
      props.put("broker.id", "123")
      props.put("port", "4242")
      props.put("zookeeper.connect", "127.0.0.1:2181")
      props.put("auto.create.topics.enable", "true")
      props.put("host.name", "127.0.0.1")
      
      val conf = new KafkaConfig(props);
      val kafka = new KafkaServerStartable(conf)
      
      new Thread(new Runnable {
		    def run() {
		            kafka.startup()
		            logger.info(s"Kafka server started. Port: ${kafka.serverConfig.advertisedPort}")
		            Thread.sleep(100000)
		    }
		  }).start()

  
}