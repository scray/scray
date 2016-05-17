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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import java.nio.file.Files
import org.apache.spark.streaming.Seconds

@RunWith(classOf[JUnitRunner])
class KafkaTests extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {

  "KafkaTest " should {
    "start Kafka" in {
       val master = "local[2]"
       val appName = "example-spark"

      val batchDuration = Seconds(1)
      val checkpointDir = Files.createTempDirectory(appName).toString

        val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)
        val ssc = new StreamingContext(conf, batchDuration)
        ssc.checkpoint(checkpointDir)
        
        

       
       Thread.sleep(100000)
    }
  }
}