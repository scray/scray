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

package scray.hdfs.coordination

import scray.hdfs.index.format.sequence.SequenceFileWriter
import java.util.UUID
import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.index.format.Writer
import java.io.InputStream

class CoordinatedWriter(private var writer: Writer, maxFileSize: Long, maxNumberOfInserts: Int, writeCoordinator: WriteCoordinator, metadata: WriteDestination) extends LazyLogging with Writer {

  def insert(id: String, updateTime: Long, data: Array[Byte]) = synchronized {
    // Check if file size limit is reached
    if(writer.getBytesWritten + data.length < maxFileSize) {
      writer.insert(id, updateTime, data)
    } else {
      writer.close
      logger.debug(s"Max file size reached. Max size ${maxFileSize}.")
      writer = writeCoordinator.getNewWriter(metadata)
      this.insert(id, updateTime, data)
    }
  }
  
  def close: Unit = synchronized {
    varIsClosed = true
    writer.close
  }
  
  def getBytesWritten: Long = {
    writer.getBytesWritten
  }
  
  def getNumberOfInserts: Int = {
    writer.getNumberOfInserts
  }
  def insert(idBlob: (String, scray.hdfs.index.format.sequence.types.Blob)): Unit = ???
  
  def insert(id: String,updateTime: Long,data: InputStream, dataSize: Int, blobSplitSize: Int): Long = {
    // Check if file size limit is reached
    if(writer.getBytesWritten + dataSize < maxFileSize) {
      writer.insert(id, updateTime, data)
    } else {
      writer.close
      logger.debug(s"Max file size reached. Max size ${maxFileSize}.")
      writer = writeCoordinator.getNewWriter(metadata)
      this.insert(id, updateTime, data)
    }
  }
  
  def insert(id: String, updateTime: Long, data: InputStream, blobSplitSize: Int): Long = ???

  def insert(key: String, data: String) = ???
}