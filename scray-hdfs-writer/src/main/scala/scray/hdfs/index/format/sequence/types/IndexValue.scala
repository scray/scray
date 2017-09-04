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
  
  def getKey: org.apache.hadoop.io.Text = {
    key
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
