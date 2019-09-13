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

import java.io.InputStream
import java.math.BigInteger
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging

import scray.hdfs.io.index.format.Writer
import scray.hdfs.io.index.format.sequence.mapping.SequenceKeyValuePair
import scray.hdfs.io.index.format.sequence.SequenceFileWriter
import org.apache.hadoop.io.Writable
import scray.hdfs.io.index.format.sequence.types.BlobKey
import scray.hdfs.io.index.format.sequence.types.IndexValue
import scray.hdfs.io.index.format.sequence.types.Blob
import org.apache.hadoop.io.Text
import scray.hdfs.io.modify.Renamer
import org.apache.hadoop.conf.Configuration
import java.util.Optional
import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.configure.FilenameCreator
import scray.hdfs.io.configure.RandomUUIDFilenameCreator
import java.util.Timer

class CoordinatedWriter[+IDXKEY <: Writable, +IDXVALUE <: Writable, +DATAKEY <: Writable, +DATAVALUE <: Writable](
  maxFileSize:    Long                                                       = Long.MaxValue,
  metadata:       WriteParameter,
  outTypeMapping: SequenceKeyValuePair[IDXKEY, IDXVALUE, DATAKEY, DATAVALUE]) extends LazyLogging with Writer {

  private var hdfsConf: Configuration = null; // Initialized when writer was created.
  private var writer: Writer = createNewBasicWriter(metadata)
  private var numInserts = 0
  private var timer = new Timer(true)
  private var timerTask: CloseFileTimer = _

  def insert(id: String, updateTime: Long, data: Array[Byte]) = synchronized {

    // Limit was reached last time.
    if (writer.isClosed) {
      writer = createNewBasicWriter(metadata)
      this.startTimer
      
      logger.debug(s"Create new file ${writer.getPath}")
    }
    logger.debug(s"Insert remaining time for time limit ${timerTask.getRemainingTime/1000}s")


    numInserts = numInserts + 1
    val writtenBytes = writer.insert(id, updateTime, data)

    // Check if file size limit is reached
    if (maxFileSizeReached(writer.getBytesWritten + data.length, maxFileSize)
      ||
      maxNumInsertsReached(numInserts, metadata.maxNumberOfInserts)) {
      logger.debug(s"Close file ${writer.getPath}")
      this.close

      numInserts = 0
    }

    writtenBytes
  }

  private def createNewBasicWriter(metadata: WriteParameter): Writer = {
    val filePath = this.getPath(metadata.path, metadata.queryspace, metadata.version.number, metadata.writeVersioned, metadata.fileNameCreator)
    val writer = new SequenceFileWriter(filePath, outTypeMapping, metadata.createScrayIndexFile, metadata.user, metadata.sequenceFileCompressionType)
    this.hdfsConf = writer.hdfsConf
    this.startTimer
    
    writer
  }

  private def getPath(basePath: String, queryspace: String, version: Int, writeVersioned: Boolean, customFileName: Optional[FilenameCreator]): String = {
    if (writeVersioned) {
      if (metadata.storeAsHiddenFileTillClosed) {
        if (basePath.endsWith("/")) {
          s"${basePath}scray-data-${queryspace}-v${version}/.${customFileName.orElse(new RandomUUIDFilenameCreator).getNextFilename}"
        } else {
          s"${basePath}/scray-data-${queryspace}-v${version}/.${customFileName.orElse(new RandomUUIDFilenameCreator).getNextFilename}"
        }
      } else {
        if (basePath.endsWith("/")) {
          s"${basePath}scray-data-${queryspace}-v${version}/${customFileName.orElse(new RandomUUIDFilenameCreator).getNextFilename}"
        } else {
          s"${basePath}/scray-data-${queryspace}-v${version}/${customFileName.orElse(new RandomUUIDFilenameCreator).getNextFilename}"
        }
      }
    } else {
      s"${basePath}${customFileName.orElse(new RandomUUIDFilenameCreator).getNextFilename}"
    }
  }

  def maxFileSizeReached(writtenBytes: Long, maxSize: Long): Boolean = {
    if (maxSize > 0) {
      logger.debug(s"Inserted bytes ${writtenBytes} of max ${maxSize} bytes")
      writtenBytes >= maxSize
    } else {
      logger.debug(s"Inserted bytes ${writtenBytes} to sequence file.")
      false
    }
  }

  def maxNumInsertsReached(numberInserts: Int, maxNumerInserts: Int): Boolean = {

    if (maxNumerInserts > 0) {
      logger.debug(s"Insert ${numberInserts}/${maxNumerInserts}")
      numberInserts >= maxNumerInserts
    } else {
      logger.debug(s"Inserted ${numberInserts} key/value pairs to sequence file.")
      false
    }
  }

  def close: Unit = synchronized {
    varIsClosed = true
    writer.close
    timer.cancel()

    if (metadata.storeAsHiddenFileTillClosed) {
      val renamer = new Renamer();

      val pathAndFilename = renamer.separateFilename(writer.getPath)
      val newFilename = pathAndFilename._1 + pathAndFilename._2.replace(".", "")

      if (metadata.createScrayIndexFile) {
        renamer.rename(writer.getPath + ".data.seq", newFilename + ".data.seq", hdfsConf).get()
        renamer.rename(writer.getPath + ".idx.seq", newFilename + ".idx.seq", hdfsConf).get()
      } else {
        if (metadata.fileNameCreator.isPresent()) {
          renamer.rename(writer.getPath, newFilename, hdfsConf).get()
        } else {
          renamer.rename(writer.getPath + ".seq", newFilename + ".seq", hdfsConf).get()
        }
      }
    }
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

  override def insert(id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): Long = synchronized {

    // Limit was reached last time.
    if (writer.isClosed) {
      writer = createNewBasicWriter(metadata)
      startTimer
      logger.debug(s"Create new file ${writer.getPath}")
    }
    logger.debug(s"Insert remaining time for time limit ${timerTask.getRemainingTime/1000}s")

    numInserts = numInserts + 1
    val writtenBytes = writer.insert(id, updateTime, data)

    // Check if file size limit is reached
    if (maxFileSizeReached(writer.getBytesWritten + dataSize.longValue(), maxFileSize)
      ||
      maxNumInsertsReached(numInserts, metadata.maxNumberOfInserts)) {
      logger.debug(s"Close file ${writer.getPath}")
      this.close

      numInserts = 0
    }

    writtenBytes
  }

  override def insert(id: String, data: String): Long = synchronized {

    // Limit was reached last time.
    if (writer.isClosed) {
      writer = createNewBasicWriter(metadata)
      startTimer
      logger.debug(s"Create new file ${writer.getPath}")
    }
    logger.debug(s"Insert remaining time for time limit ${timerTask.getRemainingTime/1000}s")

    numInserts = numInserts + 1
    val writtenBytes = writer.insert(id, data)

    // Check if file size limit is reached
    if (maxFileSizeReached(writer.getBytesWritten + data.getBytes.length, maxFileSize)
      ||
      maxNumInsertsReached(numInserts, metadata.maxNumberOfInserts)) {
      logger.debug(s"Close file ${writer.getPath}")
      this.close

      numInserts = 0
    }

    writtenBytes
  }

  def insert(id: String, updateTime: Long, data: InputStream, blobSplitSize: Int): Long = synchronized {

    // Limit was reached last time.
    if (writer.isClosed) {
      writer = createNewBasicWriter(metadata)
      this.startTimer
      logger.debug(s"Create new file ${writer.getPath}")
    }

    logger.debug(s"Insert remaining time for time limit ${timerTask.getRemainingTime/1000}s")
    
    numInserts = numInserts + 1
    val writtenBrytes = writer.insert(id, updateTime, data, blobSplitSize)

    // Check if file size limit is reached
    if (maxNumInsertsReached(numInserts, metadata.maxNumberOfInserts)) {
      logger.debug(s"Close file ${writer.getPath}")
      this.close

      numInserts = 0
    }

    writtenBrytes
  }

  private def startTimer: Unit = {
    timer = new Timer(true)
    timerTask = new CloseFileTimer(this, metadata.timeLimit)
    
    if (metadata.timeLimit > 0) {
      timer.schedule(timerTask, metadata.timeLimit)
    }
  }
}