package scray.example.job

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import java.nio.file.Files
import org.apache.spark.streaming.Seconds

class Main {
    def main(args: Array[String]): Unit = {
       val master = "local[2]"
       val appName = "example-spark"

      val batchDuration = Seconds(1)
      val checkpointDir = Files.createTempDirectory(appName).toString

        val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)
        
        val ssc = new StreamingContext(conf, batchDuration)
        ssc.checkpoint(checkpointDir)
        
        val source = StreamingDStreams.getKafkaStringSource(ssc, Some("localhost:4242"), Some(Map(("test", 1))), org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      
      // source.foreach(f => println(f.count()))
      Thread.sleep(100000)
    }
}