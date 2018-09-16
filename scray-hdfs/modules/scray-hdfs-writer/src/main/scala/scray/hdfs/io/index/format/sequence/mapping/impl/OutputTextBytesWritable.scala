package scray.hdfs.io.index.format.sequence.mapping.impl

import scray.hdfs.io.index.format.sequence.mapping.SequenceKeyValuePair
import org.apache.hadoop.io.ByteWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable

class OutputTextBytesWritable extends SequenceKeyValuePair[Text, Text, Text, BytesWritable] {
  
  def getIdxKey(id: String): Text = {
    new Text(s"{id: ${id}}")
  }
  
  def getIdxValue(id: String, blobSplits: Int, splitSize: Int, updateTime: Long, dataLength: Long): Text = {
    new Text(s"{id: $id, blobSplits: ${blobSplits}, splitSize: ${splitSize}, updateTime: ${updateTime}, dataLength: ${dataLength}}")  
  }
  
  def getIdxValue(id: String, updateTime: Long, dataLength: Long): Text = {
    this.getIdxValue(id, 1, 8192, updateTime, dataLength)
  }
  
  def getDataKey(id: String, blobCount: Int = 0): Text = {
    new Text(s"{id: ${id}, blobCount: ${blobCount}}")
  }
  
  def getDataValue(data: Array[Byte], length: Int) = {
    new BytesWritable(data, length)
  }
  
  def getDataValue(data: Array[Byte]) = {
    new BytesWritable(data, data.length)
  }
  
    def getDataValue(data: String) = {
      new BytesWritable(data.getBytes)
    }

  
}