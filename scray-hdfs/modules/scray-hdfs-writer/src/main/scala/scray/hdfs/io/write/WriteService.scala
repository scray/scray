package scray.hdfs.io.write

import java.math.BigInteger
import java.util.UUID
import java.io.InputStream

trait WriteService {
    /**
   * Create new writer instance on service side.
   * 
   * @return id to identify resource
   */
  def createWriter(path: String): UUID
  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]): Long
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024): Long
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): Long
  def getPath(resource: UUID): String
  def getBytesWritten(resource: UUID): Long
  def getNumberOfInserts(resource: UUID): Int
  def close(resource: UUID)
  def isClosed(resource: UUID)
}