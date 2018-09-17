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

package scray.hdfs.io.index.format.sequence.types

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import java.util.Arrays

class Blob(
  private var data: Array[Byte],
  private var dataLength: Int
)  extends Writable with Serializable {
  

  def this() = {
    this(Array.empty[Byte], 0)
  }
  
  def this(updateTime: Long, data: Array[Byte]) = {
    this(data, data.length)
  }


  
  def getData: Array[Byte] = {
    data
  }

  override def write(out: java.io.DataOutput): Unit = {
    out.writeInt(dataLength)
    out.write(data, 0, dataLength)
  }
  
  override def readFields(in: java.io.DataInput): Unit = {
    dataLength = in.readInt
    data = new Array[Byte](dataLength)
    in.readFully(data, 0, dataLength)
  }
  
  override def hashCode = {
    data.hashCode()
  }
  
  override def equals(that: Any): Boolean = {
    that match {
      case that: Blob => {
        Arrays.equals(that.data, this.data)
      }
      case _ => false
    }
  }
  
  override def toString(): String = {
    data.toString()
  }
}