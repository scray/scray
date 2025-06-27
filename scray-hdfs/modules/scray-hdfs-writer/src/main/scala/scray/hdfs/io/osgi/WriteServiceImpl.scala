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

package scray.hdfs.io.osgi

import java.io.InputStream
import java.math.BigInteger
import java.util.HashMap
import java.util.Optional
import java.util.UUID

import org.apache.hadoop.io.Writable
import org.slf4j.LoggerFactory

import com.google.common.util.concurrent.SettableFuture

import scray.hdfs.io.configure.Version
import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.coordination.CoordinatedWriter
import scray.hdfs.io.index.format.raw.RawFileWriter
import scray.hdfs.io.modify.Renamer
import scray.hdfs.io.write.IHdfsWriterConstats
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.write.ScrayOutputStream
import scray.hdfs.io.write.WriteResult
import scray.hdfs.io.write.WriteService
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextText
import java.net.URI

class WriteServiceImpl extends WriteService {

  val logger = LoggerFactory.getLogger(classOf[WriteServiceImpl])
  private val writersMetadata = new HashMap[UUID, CoordinatedWriter[Writable, Writable, Writable, Writable]];

  private var rawFileWriter: HashMap[Int, RawFileWriter] = new HashMap[Int, RawFileWriter]

  override def createWriter(path: String): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val config = new (WriteParameter.Builder)
      .setPath(path)
      .createConfiguration

    writersMetadata.put(id, new CoordinatedWriter(8192, config, new OutputBlob))

    id
  }

  def createWriter(path: String, format: SequenceKeyValueFormat): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val config = new (WriteParameter.Builder)
      .setPath(path)
      .setFileFormat(format)
      .createConfiguration

    this.createWriter(config)
  }

  def createWriter(metadata: WriteParameter): UUID = synchronized {
    val id = UUID.randomUUID()

    metadata.fileFormat match {
      case SequenceKeyValueFormat.SEQUENCEFILE_INDEXVALUE_BLOB    => writersMetadata.put(id, new CoordinatedWriter(metadata.maxFileSize, metadata, new OutputBlob))
      case SequenceKeyValueFormat.SEQUENCEFILE_TEXT_BYTESWRITABLE => writersMetadata.put(id, new CoordinatedWriter(metadata.maxFileSize, metadata, new OutputTextBytesWritable))
      case SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT          => writersMetadata.put(id, new CoordinatedWriter(metadata.maxFileSize, metadata, new OutputTextText))
    }

    id
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]): ScrayListenableFuture[WriteResult] = synchronized {
    logger.debug(s"Insert data for resource ${resource}.")

    try {
      val writer = this.getWriter(resource)
      val bytesWritten = writer.insert(id, updateTime, data)
      new ScrayListenableFuture(new WriteResult(writer.isClosed, "Data inserted", bytesWritten))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture[WriteResult](e)
      }
    }
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024): ScrayListenableFuture[WriteResult] = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      val writer = this.getWriter(resource)
      val bytesWritten = writer.insert(id, updateTime, data)
      new ScrayListenableFuture(new WriteResult(writer.isClosed, "Data inserted", bytesWritten))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): ScrayListenableFuture[WriteResult] = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      val writer = this.getWriter(resource)
      val bytesWritten = writer.insert(id, updateTime, data)
      new ScrayListenableFuture(new WriteResult(writer.isClosed, "Data inserted", bytesWritten))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def writeRawFile(path: String, data: InputStream, user: String, password: Array[Byte]): ScrayListenableFuture[WriteResult] = synchronized {
    try {
      val writerId = getHdfsWriterId(path, user, password)
      if (rawFileWriter.get(writerId) == null) {
        rawFileWriter.put(writerId, new RawFileWriter(path, user, password))
      }

      rawFileWriter.get(writerId).write(path, data)
      rawFileWriter.remove(writerId)
      new ScrayListenableFuture(new WriteResult("Wrote file successfully"))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  protected def getHdfsWriterId(path: String, user: String, password: Array[Byte]): Int = {
    val host = () => {
      if (path.startsWith("file:////")) {
        path.split("file:////")(1).split("/")(0)
      } else if (path.startsWith("hdfs://")) {
        path.split("hdfs://")(1).split("/")(0)
      } else if (path.startsWith("file:/")) {
        path.split("file:/")(1).split("/")(0)
      } else {
        throw new IllegalArgumentException("Unsuported path defined. Pash should start with hdfs:// or file:///. Path: " + path)
      }
    }

    val key = host() + user + (if(password == null) 7 else password.hashCode())
    key.hashCode()
  }
  
  
  def writeRawFile(path: String, user: String, password: Array[Byte]): ScrayOutputStream = synchronized {

    val writer = new RawFileWriter(path, user, password)

    new ScrayOutputStream(writer.write(path))
  }

  def close(resource: UUID) = synchronized {
    try {
      writersMetadata.get(resource).close
      writersMetadata.remove(resource)

      val result = SettableFuture.create[WriteResult]()
      result.set(new WriteResult("Data inserted"))
      result
    } catch {
      case e: Exception => {
        val result = SettableFuture.create[WriteResult]()
        result.setException(e)
        result
      }
    }
  }

  override def rename(source: String, destination: String): ScrayListenableFuture[WriteResult] = {
    logger.debug(s"Rename file from ${source} to ${destination}")
    val renamer = new Renamer
    renamer.rename(source, destination)
  }

  def deleteFile(path: String, user: String, password: Array[Byte]): ScrayListenableFuture[String] = {
    try {

      val writer = new RawFileWriter(path, user, password)

      writer.deleteFile(path)
      new ScrayListenableFuture(path)
    } catch {
      case e: Throwable => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def closeAll = synchronized {

    // Close raw file writer
    val rawWriterKeySet = rawFileWriter.keySet().iterator()
    while (rawWriterKeySet.hasNext()) {
      try {
        rawFileWriter.get(rawWriterKeySet.next()).close
        rawFileWriter.remove(rawWriterKeySet.next())
      } catch {
        case e: Exception => logger.error(s"Error while closing writer: " + e )
      }
    }

    // Close SequenceFile writer
    val keysOfWriter = writersMetadata.keySet().iterator()
    while (keysOfWriter.hasNext()) {
      val writer = writersMetadata.get(keysOfWriter.next())

      try {
        writer.close
        writersMetadata.remove(keysOfWriter.next())
      } catch {
        case e: Exception => logger.error(s"Error while closing writer ${writer}. Exception: ${e}")
      }

    }
  }

  def isClosed(resource: UUID): ScrayListenableFuture[WriteResult] = {
    try {
      val isClosed = writersMetadata.get(resource).isClosed
      new ScrayListenableFuture(new WriteResult(isClosed, "File is closed", writersMetadata.get(resource).getBytesWritten))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  private def getWriter(resource: UUID): CoordinatedWriter[Writable, Writable, Writable, Writable] = {
    if (writersMetadata.get(resource) != null) {
      writersMetadata.get(resource)
    } else {
      logger.error(s"No writer with id ${resource}. To create a writer call createWriter(...) first.")
      throw new RuntimeException(s"No writer with id ${resource}. To create a writer call createWriter(...) first.")
    }
  }
}