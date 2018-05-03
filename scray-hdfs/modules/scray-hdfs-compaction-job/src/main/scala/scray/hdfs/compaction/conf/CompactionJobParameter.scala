package scray.hdfs.compaction.conf

/**
 * container class for configuration parameters.
 */
case class CompactionJobParameter(
  var indexFilesInputPath: String   =   "hdfs://10.11.22.41/scray-data/in", 
  var indexFilesOutputPath: String  =   "hdfs://10.11.22.41/scray-data/out", 

  var dataFilesInputPath: String    =   "hdfs://10.11.22.41/scray-data/in",
  var dataFilesOutputPath: String   =   "hdfs://10.11.22.41/scray-data/out"
)