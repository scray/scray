package org.scray.example.cli

import scopt.OptionParser
import com.typesafe.scalalogging.LazyLogging

/**
 * This class is designed to handle options coming from CLI
 */
object Options extends LazyLogging {
  
  /**
   * Parser to parse CLI args
   */
  val parser = new OptionParser[Config]("facility-state-job") {
    head("facility-state-job", "1.0-SNAPSHOT")
    opt[Unit]('b', "batch").optional() action { (x, c) =>
      c.copy(batch = true) } text("provide URL to Kafka Broker if sourcing from Kafka is desired")
    opt[String]('t', "kafka-topics").optional() action { (x, c) =>
      c.copy(kafkaTopic = x) } text("provide URL to Kafka Broker if sourcing from Kafka is desired")
    opt[String]('m', "master").optional() required() action { (x, c) =>
      c.copy(master = x) } text("provide URL to Spark master")
    opt[String]('g', "graphite hostname").optional() action { (x, c) =>
      c.copy(graphiteHost = x) } text("hostname of graphite data sink. E.g. 127.0.0.1 (default: None)")
  }
  
  /**
   * parse the arguments and return Config
   */
  def parse(args: Array[String]): Option[Config] = {
//    val config = parser.parse(args, Config("master"))
//    config.flatMap { conf => 
//    val Config(master, batch) = conf
//      if(!batch) {
//        kafkaDStreamURL.flatMap { kafka => 
//          if(kafkaTopic.isDefined) {
//            config
//          } else {
//            println("No kafka topic defined")
//            None
//          }
//        }.orElse {
//          if(hdfsDStreamURL.isDefined) {
//            config  
//          } else {
//            println("No hdfs url defined")
//            None
//          }
//        }
//      } else {
//        println("Batch job")
//        config
//      }
//    }
    
//    config
    
    Some(Config())
  }
}
