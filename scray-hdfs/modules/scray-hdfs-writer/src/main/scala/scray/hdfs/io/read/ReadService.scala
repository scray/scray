package scray.hdfs.io.read

import java.io.InputStream
import java.util.UUID
import java.util.Map
import java.lang.Boolean

import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat

trait ReadService {
  def getInputStream(path: String): ScrayListenableFuture[InputStream]
  
  def readFullSequenceFile(path: String, format: SequenceKeyValueFormat):  UUID
  def hasNextSequenceFilePair(id: UUID):  ScrayListenableFuture[java.lang.Boolean]
  def getNextSequenceFilePair(id: UUID): ScrayListenableFuture[Map.Entry[String, Array[Byte]]]
  
  def getFileList(path: String): ScrayListenableFuture[java.util.List[FileParameter]]
  def deleteFile(path: String): ScrayListenableFuture[String]
}