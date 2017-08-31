package scray.hdfs.index.format.sequence.types

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable
import scala.xml.Text

class IndexValue(
    keyIn: String,
    updateTimeIn: Long, 
    positionIn: Long) extends Writable {
  private val key = new org.apache.hadoop.io.Text(keyIn)
  private val updateTime = new LongWritable(updateTimeIn)
  private val position = new LongWritable(positionIn)
  
  def this() {
    this("42", 0L, 0)
  }
  
  def readFields(data: java.io.DataInput): Unit = {
    key.set(data.readUTF())
    updateTime.set(data.readLong)
    position.set(data.readLong)
  }
  
  def write(out: java.io.DataOutput): Unit = {
    out.writeUTF(key.toString())
    out.writeLong(updateTime.get)
    out.writeLong(position.get)
  }
  
  def getKey: String = {
    key.toString()
  }
  
  def getUpdateTime: Long = {
    updateTime.get
  }
  
  def getPosition: Long = {
    position.get
  }
  
  override def toString(): String = {
    s"IndexValue{key: ${key}, updateTime: ${updateTime.get}, position: ${position.get}}"
  }

}