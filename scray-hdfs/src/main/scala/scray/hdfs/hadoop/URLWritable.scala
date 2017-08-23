package scray.hdfs.hadoop

import java.util.UUID
import java.io.DataInput
import java.io.DataOutput
import java.net.URL
import org.apache.hadoop.io.Writable

class URLWritable extends Writable {

  def this(url: URL) = {
    this()
    set(url)
  }
  
  override def write(out: DataOutput): Unit = value.foreach { url =>
    out.writeUTF(url.toString())
  }

  override def readFields(in: DataInput): Unit = {
    val str = in.readUTF()
    value = Some(new URL(str)) 
  }
  
  def set(url: URL) = {
    value = Some(url)
  }
  
  def get(): URL = value.getOrElse(null)
  
  var value: Option[URL] = None

}