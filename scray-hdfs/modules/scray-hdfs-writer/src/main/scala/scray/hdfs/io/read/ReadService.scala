package scray.hdfs.io.read

import java.io.InputStream
import scray.hdfs.io.write.ScrayListenableFuture

trait ReadService {
  def getInputStream(path: String): ScrayListenableFuture[InputStream]
  def getFileList(path: String): ScrayListenableFuture[java.util.List[String]]
  def deleteFile(path: String)
}