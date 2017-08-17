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

class BlobFileRecord(
  private val key: Array[Byte],
  private val value: Array[Byte]
 ) {
  
  private val keyLength: Int = key.length
  private val valueLength: Int = value.length

  
  def getByteRepresentation: Array[Byte] = {
    
    val buffer = ByteBuffer.allocate(
        4 + // key length
        keyLength + // bytes to store key
        4 + // value length
        valueLength // required bytes to store value
     );
    
    buffer.putInt(keyLength)
    buffer.put(key)
    
    buffer.putInt(valueLength)
    buffer.put(value)
        
    buffer.array()
  }
  
  def getKey = {
    key
  }
  
  def getValue = {
    value
  }
}

object BlobFileRecord {
  
  def apply(records: DataInputStream) = {
    val keyLength = records.readInt()
    
    val key = new Array[Byte](keyLength)
    records.read(key)
    
    val valueLength = records.readInt()
    val value = new Array[Byte](valueLength)
    records.read(value)

    new BlobFileRecord(key, value)
  }
  
  def apply(key: Array[Byte], value: Array[Byte]) = {
    new BlobFileRecord(key, value)
  }
}