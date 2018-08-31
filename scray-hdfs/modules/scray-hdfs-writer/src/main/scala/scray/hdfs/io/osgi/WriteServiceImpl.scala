package scray.hdfs.io.osgi

import java.io.InputStream
import java.math.BigInteger
import java.util.HashMap
import java.util.UUID



import org.slf4j.LoggerFactory

import scray.hdfs.io.coordination.IHdfsWriterConstats
import scray.hdfs.io.coordination.IHdfsWriterConstats.FileFormat
import scray.hdfs.io.coordination.ReadWriteCoordinatorImpl
import scray.hdfs.io.coordination.Version
import scray.hdfs.io.coordination.WriteDestination
import scray.hdfs.io.write.Failure
import scray.hdfs.io.write.Success
import scray.hdfs.io.write.WriteService
import scray.hdfs.io.write.WriteState
import scray.hdfs.io.index.format.raw.RawFileWriter

class WriteServiceImpl extends WriteService {

  val logger = LoggerFactory.getLogger(classOf[WriteServiceImpl])
  private val writersMetadata = new HashMap[UUID, WriteDestination];
  private val writeCoordinator = new ReadWriteCoordinatorImpl

  def createWriter(path: String): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, IHdfsWriterConstats.FileFormat.SequenceFile, Version(0), 512 * 1024 * 1024L, 5)
    writeCoordinator.getWriter(metadata)

    writersMetadata.put(id, metadata)

    id
  }
  
  def createWriter(path: String, format: FileFormat): UUID = synchronized {
    logger.debug(s"Create writer for path ${path}")
    val id = UUID.randomUUID()

    val metadata = WriteDestination("000", path, format, Version(0), 512 * 1024 * 1024L, 5)
    writeCoordinator.getWriter(metadata)

    writersMetadata.put(id, metadata)

    id
  }
  
  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]): WriteState = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      writeCoordinator.getWriter(writersMetadata.get(resource))
        .insert(id, updateTime, data)

      new WriteState(true, new Success)
    } catch {
      case e: Exception => new WriteState(true, new Failure(e))
    }
  }
  
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024): WriteState = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      writeCoordinator.getWriter(writersMetadata.get(resource))
        .insert(id, updateTime, data)

      new WriteState(true, new Success)
    } catch {
      case e: Exception => new WriteState(true, new Failure(e))
    }
  }
  
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): WriteState = synchronized {
    logger.debug(s"Insert data for resource ${resource}")

    try {
      writeCoordinator.getWriter(writersMetadata.get(resource))
        .insert(id, updateTime, data)

      new WriteState(true, new Success)
    } catch {
      case e: Exception => new WriteState(true, new Failure(e))
    }
  }

  def writeRawFile(path: String, data: InputStream): WriteState = synchronized {
    try{
      val writer =  new RawFileWriter(path)
      writer.write(path, data)
      writer.close
      new WriteState(true, new Success)
    } catch {
      case e: Exception => new WriteState(true, new Failure(e))
    }
  }
  
  def getPath(resource: UUID): String = {
    "42"
  }

  def getBytesWritten(resource: UUID): Long = {
    42L
  }
  def getNumberOfInserts(resource: UUID): Int = {
    42
  }

  def close(resource: UUID) = synchronized {
    try {
      writeCoordinator.getWriter(writersMetadata.get(resource))
        .close

      new WriteState(true, new Success)
    } catch {
      case e: Exception => new WriteState(true, new Failure(e))
    }
  }
  
  def isClosed(resource: UUID) = synchronized {
    try {
      writeCoordinator.getWriter(writersMetadata.get(resource))
        .isClosed

      new WriteState(true, new Success)
    } catch {
      case e: Exception => new WriteState(true, new Failure(e))
    }
  }
}