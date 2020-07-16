package com.seeburger.research.cloud.ai.cli

import scopt.OptionParser
import com.typesafe.scalalogging.LazyLogging

/**
 * This class is designed to handle options coming from CLI
 */
object Options extends LazyLogging {
  
  /**
   * Parser to parse CLI args
   */
  val parser = new OptionParser[Config]("IngestSlaSqlData") {
    head("Parameters for IngestSlaSqlData", "1.0-SNAPSHOT")
    opt[Unit]('b', "batch") action { (x, c) =>
      c.copy(batch = true) } text("provide URL to Kafka Broker if sourcing from Kafka is desired")
    opt[String]('p', "sqlPw") action { (x, c) =>
      c.copy(sqlPassword = x) } text("Password to acces sql database")
    opt[String]('u', "sqlUsr") action { (x, c) =>
      c.copy(sqlUser = x) } text("User of sql database")
    opt[String]('m', "master") required() action { (x, c) =>
      c.copy(master = x) } text("provide URL to Spark master")
  }
  
  /**
   * parse the arguments and return Config
   */
  def parse(args: Array[String]): Option[Config] = {
    val config = parser.parse(args, Config("master"))
    config.flatMap { conf => 
    val Config(master, batch, sqlUser, sqlPassword) = conf
     config
    }
  }
}
