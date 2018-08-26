package scray.hdfs.io.index.format.sequence.mapping

import org.apache.hadoop.io.Writable


trait InputOutputTypeMapping[IDXKEY <: Writable, IDXVALUE <: Writable, DATAKEY <: Writable, DATAVALUE <: Writable] {
  def getIdxKey(id: String): IDXKEY
  def getIdxValue(
      id: String, 
      blobSplits: Int = 1, 
      splitSize: Int = 8192, 
      updateTime: Long, 
      dataLength: Long): IDXVALUE
  
  def getIdxValue(
      id: String, 
      updateTime: Long, 
      dataLength: Long): IDXVALUE
      
  def getDataKey(id: String, blobCount: Int = 0): DATAKEY
  def getDataValue(data: Array[Byte]): DATAVALUE
  def getDataValue(data: Array[Byte], length: Int): DATAVALUE
}