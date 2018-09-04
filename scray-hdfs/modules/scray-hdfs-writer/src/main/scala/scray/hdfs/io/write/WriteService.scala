package scray.hdfs.io.write

import java.math.BigInteger
import java.util.UUID
import java.io.InputStream
import scray.hdfs.io.coordination.IHdfsWriterConstats.FileFormat
import java.io.OutputStream

trait WriteService {
    /**
   * Create new writer instance on service side.
   * 
   * @return id to identify resource
   */
  def createWriter(path: String): UUID 
  def createWriter(path: String, format: FileFormat): UUID
  
  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]): WriteState
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024): WriteState
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): WriteState
  
  def writeRawFile(path: String, data: InputStream): WriteState
  def writeRawFile(path: String): OutputStream
  
  def close(resource: UUID)
  def isClosed(resource: UUID)
}