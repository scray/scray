package scray.hdfs.io.write

import java.io.InputStream
import java.io.OutputStream
import java.math.BigInteger
import java.util.UUID

import com.google.common.util.concurrent.ListenableFuture
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat

trait WriteService {
    /**
   * Create new writer instance on service side.
   * 
   * @return id to identify resource
   */
  def createWriter(path: String): UUID 
  def createWriter(path: String, format: SequenceKeyValueFormat): UUID
  
  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]): ScrayListenableFuture
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 2048): ScrayListenableFuture
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): ScrayListenableFuture
  
  def writeRawFile(path: String, data: InputStream): ScrayListenableFuture
  def writeRawFile(path: String): OutputStream
  
  def close(resource: UUID)
  def isClosed(resource: UUID): ScrayListenableFuture
}