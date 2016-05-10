package scray.example.adapter

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import kafka.server.KafkaConfig
import java.util.Properties
import kafka.server.KafkaConfig

@RunWith(classOf[JUnitRunner])
class KafkaTests extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {

  "KafkaTest " should {

    "start ZooKeeper" in {
      //val zooKeeper = new ZooKeeperServer 
      
      // while(true){}
    }
    "start Kafka" in {
      
      val zooKeeper = new ZooKeeperServer 

      
      val props = new Properties()
      props.put("broker.id", "123")
      props.put("port", "4242")
     // props.put("log.dir", logDir);
      props.put("zookeeper.connect", "127.0.0.1:12345")
      props.put("host.name", "127.0.0.1")
      
      val conf = new KafkaConfig(props);
      val kafka = new KafkaServerStartable(conf)
      
      kafka.startup()
      println(kafka.serverConfig.advertisedPort)
      while(true){}
    }
  }
}