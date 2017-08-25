package scray.hdfs.index.format.sequence

import org.apache.hadoop.io.Writable
import javafx.beans.value.WritableLongValue
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable

class IndexValue(updateTimeIn: Long, positionIn: Long) extends Writable {
  val updateTime = new LongWritable(updateTimeIn)
  val position = new LongWritable(positionIn)
  
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

}