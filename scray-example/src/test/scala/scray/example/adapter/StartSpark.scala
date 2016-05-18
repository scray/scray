package scray.example.adapter

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import kafka.server.KafkaConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import scray.example.job.StreamingDStreams

class StartSpark {
  val master = "local[2]"
  val appName = "example-spark"
  
  val batchDuration = Seconds(1)
  val checkpointDir = Files.createTempDirectory(appName).toString

  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  new Thread(new Runnable {
    def run() = {
     
      val conf = new SparkConf().setMaster("local[2]").setAppName("StockAnalyser")
      val ssc = new StreamingContext(conf, Seconds(1))

      val source = StreamingDStreams.getKafkaKryoSource[String, Share](ssc, Some("localhost:2181"), Some(Map(("test", 1))), org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      source.get.foreachRDD { f => f.foreach(println(_)) }
      
      ssc.start()
      ssc.awaitTermination()
      ssc.stop()
    }
  }).start()
}