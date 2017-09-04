package org.scray.example.cli

import scopt.OptionParser
import com.typesafe.scalalogging.slf4j.LazyLogging

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
    opt[String]('k', "kafka").optional() action { (x, c) =>
      c.copy(kafkaDStreamURL = Some(x)) } text("provide URL to Kafka Broker if sourcing from Kafka is desired")
    opt[String]('t', "kafka-topics").optional() action { (x, c) =>
      c.copy(kafkaTopic = Some(x)) } text("provide URL to Kafka Broker if sourcing from Kafka is desired")
    opt[String]('h', "hdfs").optional() action { (x, c) =>
      c.copy(hdfsDStreamURL = Some(x)) } text("provide URL into HDFS if sourcing from HDFS is desired")
    opt[String]('m', "master").optional() required() action { (x, c) =>
      c.copy(master = x) } text("provide URL to Spark master")
    opt[String]('c', "cassandra").optional() action { (x, c) =>
      c.copy(cassandraHost = Some(x)) } text("provide network coordinates to a node in the Cassandra cluster")
    opt[String]('y', "keyspace").optional() action { (x, c) =>
      c.copy(cassandraKeyspace = Some(x)) } text("provide keyspace in the Cassandra cluster")
    opt[Int]('s', "seconds").optional() action { (x, c) =>
      c.copy(seconds = x) } text("number of seconds for streaming context (default: 1)")
    opt[String]('p', "checkpointPath").optional() action { (x, c) =>
      c.copy(checkpointPath = x) } text("Path (default: hdfs://localhost:8020/user/hadoop/aggmsgs)")
    opt[Int]('d', "checkpointDelay").optional() action { (x, c) =>
       c.copy(seconds = x) } text("number of milliseconds between two checkpoints (default: 10000)")
    opt[Int]('n', "numberOfBatchVersions").optional() action { (x, c) =>
      c.copy(numberOfBatchVersions = x) } text("number of versions for one batch job (default: 3)")
    opt[Int]('o', "numberOfOnlineVersions").optional() action { (x, c) =>
      c.copy(numberOfOnlineVersions = x) } text("number of versions for one online job (default: 1)")
  }
  
  /**
   * parse the arguments and return Config
   */
  def parse(args: Array[String]): Option[Config] = {
    val config = parser.parse(args, Config("master"))
    config.flatMap { conf => 
    val Config(master, batch, kafkaDStreamURL, hdfsDStreamURL, kafkaTopic, seconds, checkpointPath, checkpointDuration, cassandraHost, cassandraKeyspace, numberOfBatchVersions, numberOfOnlineVersions) = conf
      if(!batch) {
        kafkaDStreamURL.flatMap { kafka => 
          if(kafkaTopic.isDefined) {
            config
          } else {
            println("No kafka topic defined")
            None
          }
        }.orElse {
          if(hdfsDStreamURL.isDefined) {
            config  
          } else {
            println("No hdfs url defined")
            None
          }
        }
      } else {
        println("Batch job")
        config
      }
    }
  }
}
