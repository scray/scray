package scray.hdfs.hadoop

import org.apache.hadoop.io.WritableComparable
import java.util.UUID
import java.io.DataInput
import java.io.DataOutput

class UUIDWritable extends WritableComparable[UUIDWritable] {

  def this(uuid: UUID) = {
    this()
    set(uuid)
  }
  
  override def write(out: DataOutput): Unit = {
    value.foreach { uuid =>
      out.writeLong(uuid.getMostSignificantBits)
      out.writeLong(uuid.getLeastSignificantBits)
    }
  }

  override def readFields(in: DataInput): Unit = {
    val high = in.readLong()
    val low = in.readLong()
    value = Some(new UUID(high, low)) 
  }
  
  override def compareTo(to: UUIDWritable): Int = value.map(_.compareTo(to.get())).getOrElse(if(to != null) -1 else 0)

  def set(uuid: UUID) = {
    value = Some(uuid)
  }
  
  def get(): UUID = value.getOrElse(null)
  
  var value: Option[UUID] = None

}