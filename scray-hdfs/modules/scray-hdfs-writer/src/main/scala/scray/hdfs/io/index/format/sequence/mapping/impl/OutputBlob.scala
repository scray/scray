package scray.hdfs.io.index.format.sequence.mapping.impl

import scray.hdfs.io.index.format.sequence.mapping.SequenceKeyValuePair
import org.apache.hadoop.io.ByteWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import scray.hdfs.io.index.format.sequence.types.IndexValue
import scray.hdfs.io.index.format.sequence.types.BlobKey
import scray.hdfs.io.index.format.sequence.types.Blob

class OutputBlob extends SequenceKeyValuePair[Text, IndexValue, BlobKey, Blob] {
  
  def getIdxKey(id: String): Text = {
    new Text(s"{id: ${id}}")
  }
  
  def getIdxValue(id: String, blobSplits: Int, splitSize: Int, updateTime: Long, dataLength: Long): IndexValue = {
    new IndexValue(id, blobSplits, splitSize, updateTime, dataLength)
  }
  
  def getIdxValue(id: String, updateTime: Long, dataLength: Long): IndexValue = {
    this.getIdxValue(id, 1, 8192, updateTime, dataLength)
  }
  
  def getDataKey(id: String, blobCount: Int = 0): BlobKey = {
    new BlobKey(id, blobCount)
  }
  
  def getDataValue(data: Array[Byte], length: Int) = {
    new Blob(data, length)
  }
  
  def getDataValue(data: Array[Byte]) = {
    new Blob(data, data.length)
  }
  
}