package org.scray.example.conf

/**
 * container class for command line arguments. Change this along with the command line parser.
 */
case class JobParameter(
  master: String  = "yarn-cluster",                             // Spark master URL
  batch: Boolean = false,

  graphiteHost: String   =   "10.11.22.36",        
  graphitePort: Int      =   2003,
  
  kafkaBootstrapServers: String  =  "10.11.22.34:9092",
  kafkaTopic: String             =  "facility",
  
  windowDuration: String  = "20 seconds",
  slideDuration: String   = "20 seconds",
  watermark: String       = "0 milliseconds",
  
  kafkaDataSchemaAsJsonExample: String = """{"type":"struct","fields":[{"name":"type","type":"string","nullable":true,"metadata":{}},{"name":"state","type":"string","nullable":true,"metadata":{}},{"name":"timestamp","type":"long","nullable":true,"metadata":{}}]}"""
)