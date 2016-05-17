package scray.example.adapter

import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import kafka.server.KafkaConfig
import java.util.Properties
import kafka.server.KafkaConfig
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class StartSpark {
  val master = "local[2]"
  val appName = "example-spark"

  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  new Thread(new Runnable {
    def run() = {
      var sc: SparkContext = new SparkContext(conf)
      Thread.sleep(100000)
      sc.stop()
    }
  })
}