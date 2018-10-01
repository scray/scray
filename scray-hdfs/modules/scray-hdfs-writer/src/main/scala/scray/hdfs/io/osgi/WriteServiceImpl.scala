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
import scray.hdfs.io.write.IHdfsWriterConstats.FileFormat;
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

class WriteServiceImpl extends WriteService {

  val logger = LoggerFactory.getLogger(classOf[WriteServiceImpl])
  private val writersMetadata = new HashMap[UUID, CoordinatedWriter[Writable, Writable, Writable, Writable]];
    
  def createWriter(path: String): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, IHdfsWriterConstats.FileFormat.SequenceFile_IndexValue_Blob, Version(0), 512 * 1024 * 1024L, 5)

    writersMetadata.put(id,  new CoordinatedWriter(8192, metadata, new OutputBlob))

    id
  }

  def createWriter(path: String, format: FileFormat): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, format, Version(0), 512 * 1024 * 1024L, 5)

    
    format  match {
      case FileFormat.SequenceFile_IndexValue_Blob =>     writersMetadata.put(id,  new CoordinatedWriter(8192, metadata, new OutputBlob))
      case FileFormat.SequenceFile_Text_BytesWritable =>     writersMetadata.put(id,  new CoordinatedWriter(8192, metadata, new OutputTextBytesWritable))
      case FileFormat.SequenceFile_Text_Text =>     writersMetadata.put(id,  new CoordinatedWriter(8192, metadata, new OutputTextText))
    }
    

    id
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]) = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      writersMetadata.get(resource).insert(id, updateTime, data)

      new ScrayListenableFuture(new WriteResult)
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024) = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      writersMetadata.get(resource).insert(id, updateTime, data)

    new ScrayListenableFuture(new WriteResult)
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int) = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      writersMetadata.get(resource).insert(id, updateTime, data)

        new ScrayListenableFuture(new WriteResult)
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def writeRawFile(path: String, data: InputStream) = synchronized {
    try {
      val writer = new RawFileWriter(path)
      writer.write(path, data)
      
      new ScrayListenableFuture(new WriteResult)
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def writeRawFile(path: String): OutputStream = synchronized {
    val writer = new RawFileWriter(path)
    writer.write(path)
  }

  def close(resource: UUID) = synchronized {
    try {
      writersMetadata.get(resource).close

      val result = SettableFuture.create[WriteResult]()
      result.set(new WriteResult)
      result
    } catch {
      case e: Exception => {
        val result = SettableFuture.create[WriteResult]()
        result.setException(e)
        result
      }
    }
  }
  
  def closeAll = synchronized {
    val keysOfWriter = writersMetadata.keySet().iterator()
    
    while(keysOfWriter.hasNext()) {
      val writer = writersMetadata.get(keysOfWriter.next())

      try {
        writer.close
      } catch {
        case e: Exception => logger.error(s"Error while closing writer ${writer}")
      }
      
    }
  }

  def isClosed(resource: UUID): ScrayListenableFuture = {
    try {
      val isClosed = writersMetadata.get(resource).isClosed
        new ScrayListenableFuture(new WriteResult(isClosed))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }
}