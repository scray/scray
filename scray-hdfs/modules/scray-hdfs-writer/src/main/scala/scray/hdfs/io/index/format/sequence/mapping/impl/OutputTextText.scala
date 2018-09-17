package scray.hdfs.io.index.format.sequence.mapping.impl

import org.apache.hadoop.io.Text

import scray.hdfs.io.index.format.sequence.mapping.SequenceKeyValuePair

class OutputTextText extends SequenceKeyValuePair[Text, Text, Text, Text] {
  
  def getIdxKey(id: String): Text = {
    new Text(s"{id: ${id}}")
  }
  
  def getIdxValue(id: String, blobSplits: Int, splitSize: Int, updateTime: Long, dataLength: Long): Text = {
    new Text(s"{id: $id, blobSplits: ${blobSplits}, splitSize: ${splitSize}, updateTime: ${updateTime}, dataLength: ${dataLength}}")  
  }
  
  def getIdxValue(id: String, updateTime: Long, dataLength: Long): Text = {
    this.getIdxValue(id, 1, 8192, updateTime, dataLength)
  }
  
  def getDataKey(id: String, blobCount: Int): Text = {
    //new Text(s"{id: ${id}, blobCount: ${blobCount}}")

    new Text(s"{id: ${id}} ")
  }
  
  def getDataValue(data: Array[Byte], length: Int): Text = {
    new Text(new String(data, length))
  }
  
  def getDataValue(data: Array[Byte]): Text = {
    new Text(new String(data))
  }
  
  def getDataValue(data: String): Text = {
    new Text(data)
  }
}