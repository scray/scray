package scray.hdfs.io.osgi

import scray.hdfs.io.read.ReadService
import scray.hdfs.io.index.format.raw.RawFileReader
import java.util.HashMap
import java.net.URI
import scray.hdfs.io.write.ScrayListenableFuture

class ReadServiceImpl extends ReadService {
  val reader = new HashMap[String,RawFileReader]()
  
  def getFileList(path: String): ScrayListenableFuture[java.util.List[String]] = {
    try {
      val result = reader.get(this.getAuthority(path)).getFileList(path).get
      return new ScrayListenableFuture[java.util.List[String]](result)
    } catch {
      case e: Throwable => return new ScrayListenableFuture[java.util.List[String]](e)
    }
    
  }
  def getInputStream(path: String): ScrayListenableFuture[java.io.InputStream] = {
    try {
      val stream = reader
      .get(getAuthority(path))
      .read(path)
      new ScrayListenableFuture(stream)
    } catch {
      case e: Throwable => new ScrayListenableFuture(e)
    }
  }
  
  private def getAuthority(path: String): String = {
    val uri = new URI(path)
    println(uri.getAuthority)
    uri.getAuthority
  }
  
    def deleteFile(path: String){}
}