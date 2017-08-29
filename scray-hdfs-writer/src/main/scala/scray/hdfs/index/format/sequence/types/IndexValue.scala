package scray.hdfs.index.format.sequence.types

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable

class IndexValue(updateTimeIn: Long, positionIn: Long) extends Writable {
  private val updateTime = new LongWritable(updateTimeIn)
  private val position = new LongWritable(positionIn)
  
  def this() {
    this(0L, 0)
  }
  
  def readFields(data: java.io.DataInput): Unit = {
    updateTime.set(data.readLong)
    position.set(data.readLong)
  }
  def write(out: java.io.DataOutput): Unit = {
    out.writeLong(updateTime.get)
    out.writeLong(position.get)
  }
  
  def getUpdateTime: Long = {
    updateTime.get
  }
  
  def getPosition: Long = {
    position.get
  }
  
  override def toString(): String = {
    s"IndexValue{updateTime: ${updateTime.get}, position: ${position.get}}"
  }

}