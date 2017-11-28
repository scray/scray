package org.scray.example.conf

/**
 * container class for command line arguments. Change this along with the command line parser.
 */
case class JobParameter(
  var master: String  = "yarn-cluster",                             // Spark master URL
  var batch: Boolean = false,

  var graphiteHost: String   =   "10.11.22.36",        
  var graphitePort: Int      =   2003,
  
  var kafkaBootstrapServers: String  =  "10.11.22.34:9092",
  var kafkaTopic: String             =  "facility",
  
  var windowDuration: String  = "20 seconds",
  var slideDuration: String   = "20 seconds",
  var watermark: String       = "0 milliseconds",
  
  var kafkaDataSchemaAsJsonExample: String = """{"type":"struct","fields":[{"name":"type","type":"string","nullable":true,"metadata":{}},{"name":"state","type":"string","nullable":true,"metadata":{}},{"name":"timestamp","type":"long","nullable":true,"metadata":{}}]}"""
)