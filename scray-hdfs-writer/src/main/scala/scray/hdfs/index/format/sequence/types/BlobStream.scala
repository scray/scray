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

import java.io.InputStream

class BlobStream extends Blob[InputStream] {
  var updateTime: Long = -1
  var data: InputStream = null
  var dataHashcode: Int = 0
  
  def this(updateTime: Long, data: InputStream, size: Long, dataHashcode: Int) = {
    this
    
    this.updateTime = updateTime
    this.data = data
    this.dataSize = size
    this.dataHashcode = dataHashcode
  }

  override def getUpdateTime: Long = {
    updateTime
  }
  
  override def getData: InputStream = {
    data
  }

  override def write(out: java.io.DataOutput): Unit = {
    out.writeLong(updateTime)
    out.writeLong(dataSize)
    
    if(data != null) {    // Write input stream if API is used with InputStream
      logger.debug(s"Write data stream of blob")
      val buffer = new Array[Byte](0xFFFF)
      var readDataLen = data.read(buffer)
      while(readDataLen != -1) {
        out.write(buffer, 0, readDataLen)
      } 
    }
  }
  
  override def readFields(in: java.io.DataInput): Unit = {
    updateTime = in.readLong()
    val dataSize = in.readInt
    data = in.asInstanceOf[InputStream]
  }
  
  override def hashCode = {
    updateTime.hashCode() * 17 + dataHashcode
  }
  
  override def equals(that: Any): Boolean  = {
    that match {
      case that: BlobStream => {
        that.updateTime == this.updateTime &&
        that.dataHashcode == this.dataHashcode
      }
      case _ => false
    }
  }
  
  override def toString(): String = {
    updateTime + "\t" + data.toString()
  }
}