package scray.hdfs.io.read

import java.io.InputStream
import java.util.UUID
import java.util.Map
import java.lang.Boolean

import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat

trait ReadService {
  def getInputStream(path: String, user: String, password: Array[Byte]): ScrayListenableFuture[InputStream]
  
  def readFullSequenceFile(path: String, format: SequenceKeyValueFormat, user: String, password: Array[Byte]):  UUID
  def hasNextSequenceFilePair(id: UUID):  ScrayListenableFuture[java.lang.Boolean]
  def getNextSequenceFilePair(id: UUID): ScrayListenableFuture[Map.Entry[String, Array[Byte]]]
  def close(id: UUID)
  
  def getFileList(path: String, user: String, password: Array[Byte]): ScrayListenableFuture[java.util.List[FileParameter]]
}