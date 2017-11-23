package org.scray.example.cli

/**
 * container class for command line arguments. Change this along with the command line parser.
 */
case class Config(
  master: String  = "yarn-cluster",                             // Spark master URL
  batch: Boolean = false,
//  kafkaDStreamURL: Option[String] = None,     // Zookeeper-URL for sourcing from Kafka
//  hdfsDStreamURL: Option[String] = None,      // HDFS-URL for sourcing from HDFS
                  // topic-name for Kafka message queue
//  seconds: Int = 1,                           // number of seconds for Spark
//  checkpointPath: String = "hdfs://localhost:8020/user/hadoop/aggmsgs", //
//  checkpointDuration: Int = 10000,           // Duration between to checkpoints
//  cassandraHost: Option[String] = None,
//  cassandraKeyspace: Option[String] = None,
//  numberOfBatchVersions: Int = 3,
//  numberOfOnlineVersions: Int = 1,

  graphiteHost: String   =   "10.11.22.36",        // Hostname of graphite data sink. E.g. 127.0.0.1
  graphitePort: Int      =   2003,
  
  kafkaBootstrapServers: String  =  "10.11.22.34:9092",
  kafkaTopic: String             =  "facility",
  
  windowDuration: String  = "20 seconds",
  slideDuration: String   = "20 seconds",
  watermark: String       = "0 milliseconds",
  
  kafkaDataSchemaAsJsonExample: String = """{"type":"struct","fields":[{"name":"type","type":"string","nullable":true,"metadata":{}},{"name":"state","type":"string","nullable":true,"metadata":{}},{"name":"timestamp","type":"long","nullable":true,"metadata":{}}]}"""
)