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

package scray.hdfs.io.coordination

import scray.hdfs.io.index.format.sequence.BinarySequenceFileWriter
import java.util.UUID
import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.io.index.format.Writer
import java.io.InputStream
import java.math.BigInteger

class CoordinatedWriter(private var writer: Writer, maxFileSize: Long, writeCoordinator: WriteCoordinator, metadata: WriteDestination) extends LazyLogging with Writer {
  private var numInserts = 0

  def insert(id: String, updateTime: Long, data: Array[Byte]) = synchronized {
    // Check if file size limit is reached
    if (!maxFileSizeReached(writer.getBytesWritten + data.length, maxFileSize)
      &&
      !maxNumInsertsReached(numInserts, metadata.maxNumberOfInserts)) {
      numInserts = numInserts + 1
      writer.insert(id, updateTime, data)
    } else {
      logger.debug(s"Close file ${writer.getPath}")
      writer.close
      
      writer = createNewBasicWriter(metadata)
      logger.debug(s"Create new file ${writer.getPath}")
      numInserts = 0
      this.insert(id, updateTime, data)
    }
  }

  private def createNewBasicWriter(metadata: WriteDestination): Writer = {
    val filePath = this.getPath(metadata.path, metadata.queryspace, metadata.version.number)
    new BinarySequenceFileWriter(filePath)
  }

  private def getPath(basePath: String, queryspace: String, version: Int): String = {
    if (basePath.endsWith("/")) {
      s"${basePath}scray-data-${queryspace}-v${version}/${UUID.randomUUID()}"
    } else {
      s"${basePath}/scray-data-${queryspace}-v${version}/${UUID.randomUUID()}"
    }
  }
  
  def maxFileSizeReached(writtenBytes: Long, maxSize: Long): Boolean = {
    writtenBytes >= maxSize
  }

  def maxNumInsertsReached(numberInserts: Int, maxNumerInserts: Int): Boolean = {
    numberInserts >= maxNumerInserts
  }

  def close: Unit = synchronized {
    varIsClosed = true
    writer.close
  }

  def getPath: String = {
    this.writer.getPath  
  }
  
  def getBytesWritten: Long = {
    writer.getBytesWritten
  }

  def getNumberOfInserts: Int = {
    writer.getNumberOfInserts
  }
  
  override def insert(id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): Long = {

    // Check if file size limit is reached
    if (!maxFileSizeReached(writer.getBytesWritten + dataSize.longValue(), maxFileSize)
      &&
      !maxNumInsertsReached(numInserts, metadata.maxNumberOfInserts)) {
      numInserts = numInserts + 1
      writer.insert(id, updateTime, data)
    } else {
      logger.debug(s"Close file ${writer.getPath}")
      writer.close
      
      logger.debug(s"Create new file ${writer.getPath}")
      writer = createNewBasicWriter(metadata)
      numInserts = 0
      this.insert(id, updateTime, data)
    }
  }

  def insert(id: String, updateTime: Long, data: InputStream, blobSplitSize: Int): Long = {
    // Check if file size limit is reached
    if (!maxNumInsertsReached(numInserts, metadata.maxNumberOfInserts)) {
      numInserts = numInserts + 1
      writer.insert(id, updateTime, data, blobSplitSize)
    } else {
      logger.debug(s"Close file ${writer.getPath}")
      writer.close

      writer = createNewBasicWriter(metadata)
      logger.debug(s"Create new file ${writer.getPath}")

      numInserts = 0
      this.insert(id, updateTime, data, blobSplitSize)
    }
  }
}