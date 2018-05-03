package scray.hdfs.compaction.cli

/**
 * container class for command line arguments. Change this along with the command line parser.
 */
case class Config(
  master: String,                             // Spark master URL
  batch: Boolean = false,
  kafkaDStreamURL: Option[String] = None,     // Zookeeper-URL for sourcing from Kafka
  hdfsDStreamURL: Option[String] = None,      // HDFS-URL for sourcing from HDFS
  kafkaTopic: Option[Array[String]] = None,   // topic-name for Kafka message queue
  seconds: Int = 1,                           // number of seconds for Spark
  checkpointPath: String = "hdfs://localhost:8020/user/hadoop/aggmsgs", //
  checkpointDuration: Int = 10000,           // Duration between to checkpoints
  cassandraHost: Option[String] = None,
  cassandraKeyspace: Option[String] = None,
  numberOfBatchVersions: Int = 3,
  numberOfOnlineVersions: Int = 1
)