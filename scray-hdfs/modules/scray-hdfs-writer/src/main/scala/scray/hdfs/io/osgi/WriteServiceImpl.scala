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
import java.util.UUID

import org.slf4j.LoggerFactory

import scray.hdfs.io.coordination.Version
import scray.hdfs.io.coordination.WriteDestination
import scray.hdfs.io.write.WriteService
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scray.hdfs.io.index.format.raw.RawFileWriter
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import java.io.OutputStream
import com.google.common.util.concurrent.SettableFuture
import scray.hdfs.io.write.WriteResult
import scray.hdfs.io.write.WriteResult
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.coordination.CoordinatedWriter
import org.apache.hadoop.io.Writable
import java.io.PrintWriter
import scray.hdfs.io.write.IHdfsWriterConstats
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextText
import scray.hdfs.io.write.ScrayOutputStream
import scray.hdfs.io.coordination.WriteDestination
import scray.hdfs.io.modify.Renamer
import org.apache.hadoop.conf.Configuration

class WriteServiceImpl extends WriteService {

  val logger = LoggerFactory.getLogger(classOf[WriteServiceImpl])
  private val writersMetadata = new HashMap[UUID, CoordinatedWriter[Writable, Writable, Writable, Writable]];

  override def createWriter(path: String): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, None, IHdfsWriterConstats.SequenceKeyValueFormat.SequenceFile_IndexValue_Blob, Version(0), false, 512 * 1024 * 1024L, 5, true, false)

    writersMetadata.put(id, new CoordinatedWriter(8192, metadata, new OutputBlob))

    id
  }

  def createWriter(path: String, format: SequenceKeyValueFormat): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, None, format, Version(0), false, 512 * 1024 * 2048L, 50)

    this.createWriter(format, metadata)
  }

  override def createWriter(path: String, format: SequenceKeyValueFormat, numberOpKeyValuePairs: Int, customName: String): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, Some(customName), format, Version(0), false, 512 * 1024 * 2048L, numberOpKeyValuePairs, false, false)

    this.createWriter(format, metadata)
  }

  def createWriter(format: SequenceKeyValueFormat, metadata: WriteDestination): UUID = synchronized {
    val id = UUID.randomUUID()

    format match {
      case SequenceKeyValueFormat.SequenceFile_IndexValue_Blob    => writersMetadata.put(id, new CoordinatedWriter(0, metadata, new OutputBlob))
      case SequenceKeyValueFormat.SequenceFile_Text_BytesWritable => writersMetadata.put(id, new CoordinatedWriter(0, metadata, new OutputTextBytesWritable))
      case SequenceKeyValueFormat.SequenceFile_Text_Text          => writersMetadata.put(id, new CoordinatedWriter(0, metadata, new OutputTextText))
    }

    id
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]):  ScrayListenableFuture[WriteResult] = synchronized {
    logger.debug(s"Insert data for resource ${resource}.")

    try {
      this.getWriter(resource).insert(id, updateTime, data)
      new ScrayListenableFuture(new WriteResult("Data inserted"))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture[WriteResult](e)
      }
    }
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024):  ScrayListenableFuture[WriteResult] = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      this.getWriter(resource).insert(id, updateTime, data)
      new ScrayListenableFuture(new WriteResult("Data inserted"))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int):  ScrayListenableFuture[WriteResult] = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      this.getWriter(resource).insert(id, updateTime, data)
      new ScrayListenableFuture(new WriteResult("Data inserted"))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def writeRawFile(path: String, data: InputStream):  ScrayListenableFuture[WriteResult] = synchronized {
    try {
      val writer = new RawFileWriter(path)
      writer.write(path, data)

      new ScrayListenableFuture(new WriteResult("Data inserted"))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def writeRawFile(path: String): ScrayOutputStream = synchronized {
    val writer = new RawFileWriter(path)
    new ScrayOutputStream(writer.write(path))
  }

  def close(resource: UUID) = synchronized {
    try {
      writersMetadata.get(resource).close

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

  override def rename(source: String, destination: String):  ScrayListenableFuture[WriteResult] = {
    logger.debug(s"Rename file from ${source} to ${destination}")
    val renamer = new Renamer
    renamer.rename(source, destination)
  }

  def closeAll = synchronized {
    val keysOfWriter = writersMetadata.keySet().iterator()

    while (keysOfWriter.hasNext()) {
      val writer = writersMetadata.get(keysOfWriter.next())

      try {
        writer.close
      } catch {
        case e: Exception => logger.error(s"Error while closing writer ${writer}")
      }

    }
  }

  def isClosed(resource: UUID):  ScrayListenableFuture[WriteResult] = {
    try {
      val isClosed = writersMetadata.get(resource).isClosed
      new ScrayListenableFuture(new WriteResult(isClosed, "File is closed"))
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