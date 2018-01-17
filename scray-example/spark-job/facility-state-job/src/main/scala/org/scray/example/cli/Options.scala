package org.scray.example.cli

import org.scray.example.conf.JobParameter

import com.typesafe.scalalogging.LazyLogging

import scopt.OptionParser

/**
 * This class is designed to handle options coming from CLI
 */
object Options extends LazyLogging {
  
  /**
   * Parser to parse CLI args
   */
  val parser = new OptionParser[CliParameters]("facility-state-job") {
    head("facility-state-job", "1.0-SNAPSHOT")
    opt[Unit]('b', "batch").optional() action { (x, c) =>
      c.copy(batch = true) } text("provide URL to Kafka Broker if sourcing from Kafka is desired")
    opt[String]('m', "master").optional() action { (x, c) =>
      c.copy(sparkMaster = x) } text("provide URL to Spark master")
    opt[String]('c', "conf").optional() action { (x, c) =>
      c.copy(confFilePath = Some(x)) } text("job configuration yaml")
    opt[String]('d', "cassandraSeed").optional() action { (x, c) =>
      c.copy(cassandraSeed = x) } text("seed to cassandra host")
    opt[Unit]('s', "sqlStreamingJob").optional() action { (x, c) =>
      c.copy(useSparkSQLJob = true) } text("start sql streaming job")
  }
  
  /**
   * parse the arguments and return Config
   */
  def parse(args: Array[String]): Option[CliParameters] = {
    val config = parser.parse(args, CliParameters())
    config.flatMap { conf => 
    val CliParameters(batch, sparkMaster, checkpointPath, cassandraSeed, useSparkSQLJob) = conf
      config
    }
    
    config
  }
}
