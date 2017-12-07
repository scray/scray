package org.scray.example.conf

/**
 * container class for command line arguments. Change this along with the command line parser.
 */
case class JobParameter(
  var master: String  = "yarn-cluster",                             // Spark master URL
  var batch: Boolean = false,

  var graphiteHost: String   =   "127.0.0.1",        
  var graphitePort: Int      =   2003,
  var graphiteRetries: Int   =   20,              // Number of retries after an connection exception.
  
  var kafkaBootstrapServers: String  =  "127.0.0.1:9092",
  var kafkaTopic: String             =  "facility",
  
  var windowDuration: String  = "20 seconds",
  var slideDuration: String   = "20 seconds",
  var watermark: String       = "0 milliseconds",
  
  var kafkaDataSchemaAsJsonExample: String = """{"type":"struct","fields":[{"name":"type","type":"string","nullable":true,"metadata":{}},{"name":"state","type":"string","nullable":true,"metadata":{}},{"name":"timestamp","type":"long","nullable":true,"metadata":{}}]}""",

  var checkpointPath: String        = "hdfs://hdfs-namenode:8020/checkpoints-facility-state-job/",
  var sparkBatchSize: Int           = 20,
  var numberOfBatchVersions: Int    = 2,
  var numberOfOnlineVersions: Int   = 1
  
  
)