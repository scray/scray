package scray.hdfs.io.osgi

import java.io.InputStream
import java.math.BigInteger
import java.util.HashMap
import java.util.UUID

import org.slf4j.LoggerFactory

import scray.hdfs.io.coordination.IHdfsWriterConstats
import scray.hdfs.io.coordination.IHdfsWriterConstats.FileFormat
import scray.hdfs.io.coordination.Version
import scray.hdfs.io.coordination.WriteDestination
import scray.hdfs.io.write.WriteService
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

class WriteServiceImpl extends WriteService {

  val logger = LoggerFactory.getLogger(classOf[WriteServiceImpl])
  private val writersMetadata = new HashMap[UUID, CoordinatedWriter[Writable, Writable, Writable, Writable]];

  def createWriter(path: String): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, IHdfsWriterConstats.FileFormat.SequenceFile, Version(0), 512 * 1024 * 1024L, 5)

    writersMetadata.put(id,  new CoordinatedWriter(8192, metadata, new OutputBlob))

    id
  }

  def createWriter(path: String, format: FileFormat): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, format, Version(0), 512 * 1024 * 1024L, 5)

    writersMetadata.put(id,  new CoordinatedWriter(8192, metadata, new OutputBlob)) // Fix make OutputType configurarble

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