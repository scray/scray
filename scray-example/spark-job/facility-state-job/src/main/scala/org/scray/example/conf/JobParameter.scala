package org.scray.example.conf

/**
 * container class for command line arguments. Change this along with the command line parser.
 */
case class JobParameter(
  var sparkMaster: String    = "yarn",              // Spark master URL
  var batch: Boolean         = false,
  var batchFilePath: String  = "hdfs://hdfs-namenode:8020/data/Facilities*",
  var batchDataSource: BatchDataSource = TEXT,             // TEXT, CASSANDRA              
  
  var sparkStreamingBatchSize: Int = 20,
  
  var cassandraKeyspace: String  = "db",
  var cassandraTable: String     = "facility",
  

  var graphiteHost: String   =   "127.0.0.1",        
  var graphitePort: Int      =   2003,
  var graphiteRetries: Int   =   20,              // Number of retries after an connection exception.
  
  var kafkaBootstrapServers: String  =  "127.0.0.1:9092",
  var kafkaTopic: String             =  "Facilities",
  
  var windowDuration: String  = "20 seconds",
  var slideDuration: String   = "20 seconds",
  var windowStartTime: Long   =  19999,           // Microsecond in sliding interval to start windows
  var watermark: String       = "0 milliseconds",
  
  var maxDistInWindow: Int    = 5000,             // Max milliseconds between the first and last element in one time window. (Is used locally on one node)
 
  var checkpointPath: String        = "hdfs://hdfs-namenode:8020/checkpoints-facility-state-job/",
  var sparkBatchSize: Int           = 20,
  var numberOfBatchVersions: Int    = 2,
  var numberOfOnlineVersions: Int   = 1
)

abstract class BatchDataSource
case object TEXT extends BatchDataSource
case object CASSANDRA extends BatchDataSource