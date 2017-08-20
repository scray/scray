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
package scray.hdfs.index.format

import java.nio.ByteBuffer
import java.io.DataInputStream

class IndexFileRecord(
  private val key: Array[Byte],
  private val valueLength: Int,
  private val startPositon: Long // Last byte of previous byte record
 ) extends HasByteRepresentation {
  
  private val keyLength: Int =  key.length
    
  override def getByteRepresentation: Array[Byte] = {
    
    val buffer = ByteBuffer.allocate(
        4 +   // key length
        keyLength + // bytes to store key   
        8     // position in blob file [bytes]
     );
    
    buffer.putInt(keyLength)
    buffer.put(key)
    
    buffer.putLong(
        startPositon + 
        4 +   // key length information
        keyLength +
        4 +   // value length information
        valueLength
    )
    buffer.array
  }
  
  def getKey = {
    key
  }
  
  def getValueLength = {
    valueLength
  }
  
  def getStartPosition = {
    startPositon
  }
}

object IndexFileRecord {
  
  def apply(records: DataInputStream) = {
    val keyLength = records.readInt
    
    val key = new Array[Byte](keyLength)
    records.read(key)
    
    val position = records.readLong()
    
    new IndexFileRecord(key, 0, position)
  }
  
  def apply(key: Array[Byte], startPossiton: Long, valueLength: Int) = {
    val keyLength = key.length
    
    new IndexFileRecord(key, valueLength, startPossiton)
  }
}