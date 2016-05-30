//package scray.example.adapter
//
//import org.junit.runner.RunWith
//import org.scalatest.BeforeAndAfter
//import org.scalatest.BeforeAndAfterAll
//import org.scalatest.WordSpec
//import org.scalatest.junit.JUnitRunner
//
//import kafka.server.KafkaConfig
//import kafka.server.KafkaServerStartable
//import kafka.server.KafkaConfig
//import java.util.Properties
//import kafka.server.KafkaConfig
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.StreamingContext
//import java.nio.file.Files
//import org.apache.spark.streaming.Seconds
//import scray.example.job.stock
//
//@RunWith(classOf[JUnitRunner])
//class KafkaTests extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {
//
//  "KafkaTest " should {
//    "start Kafka" in {
//       stock.main(Array("--master", "spark://localhost:7077", "-b"))
//       Thread.sleep(10000)
//    }
//  }
//}