// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.hdfs.index.format.sequence.types

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import org.apache.hadoop.io.Text

class IndexValue(
    private var key: String,
    private var blobSplits: Int,
    private var splitSize: Int,
    private var updateTime: Long, 
    private var position: Long) extends Writable with Serializable {
  
  def this(
    key: String,
    updateTime: Long,
    position: Long
  ) {
    this(key, 0, 0, updateTime, position)
  }
  
  def this() {
    this("42", 0, 0, 0L, 0)
  }
  
  def readFields(data: java.io.DataInput): Unit = {
    key = data.readUTF()
    blobSplits = data.readInt()
    splitSize = data.readInt()
    updateTime = data.readLong
    position = data.readLong
  }
  
  def write(out: java.io.DataOutput): Unit = {
    out.writeUTF(key)
    out.writeInt(blobSplits)
    out.writeInt(splitSize)
    out.writeLong(updateTime)
    out.writeLong(position)
  }
  
  def getKey: String = {
    key
  }
  
  def getUpdateTime: Long = {
    updateTime
  }
  
  def getPosition: Long = {
    position
  }
  
  def getBlobSplits: Int = {
    blobSplits
  }
  
  def getSplitSize: Int = {
    splitSize
  }
  
  override def equals(that: Any): Boolean = {
    that match {
      case that: IndexValue => {
        that.getKey == this.getKey && 
        that.updateTime == this.updateTime &&
        that.position == this.position
      }
      case _ => false
    }
  }

  override def hashCode: Int = {
    return (key.toString() + updateTime.toString()).hashCode()
  }
  
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = synchronized {
    out.writeUTF(key.toString())
    out.writeInt(blobSplits)
    out.writeInt(splitSize)
    out.writeLong(updateTime)
    out.writeLong(position)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = synchronized {
    key        = in.readUTF()
    blobSplits = in.readInt()
    splitSize  = in.readInt()
    updateTime = in.readLong()
    position   = in.readLong()
  }
  
  override def toString(): String = {
    s"IndexValue{key: ${key}, blobSplits: ${blobSplits}, updateTime: ${updateTime}, position: ${position}}"
  }

}
