package scray.hdfs.io.osgi

import scray.hdfs.io.read.ReadService
import scray.hdfs.io.index.format.raw.RawFileReader
import java.util.HashMap
import java.net.URI
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.read.FileParameter

class ReadServiceImpl extends ReadService {
  val reader = new HashMap[String, RawFileReader]()

  def getFileList(path: String): ScrayListenableFuture[java.util.List[FileParameter]] = {
    try {
      if (reader.get(this.getAuthority(path)) == null) {
        reader.put(this.getAuthority(path), new RawFileReader(path))
      }
      val result = reader.get(this.getAuthority(path)).getFileList(path).get
      return new ScrayListenableFuture[java.util.List[FileParameter]](result)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        new ScrayListenableFuture[java.util.List[FileParameter]](e)
      }
    }

  }
  def getInputStream(path: String): ScrayListenableFuture[java.io.InputStream] = {
    try {
      if (reader.get(this.getAuthority(path)) == null) {
        reader.put(getAuthority(path), new RawFileReader(path))
      }

      val stream = reader
        .get(getAuthority(path))
        .read(path)
      new ScrayListenableFuture(stream)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        new ScrayListenableFuture(e)
      }
    }
  }

  override def deleteFile(path: String): ScrayListenableFuture[Unit] = {
    try {
      if (reader.get(this.getAuthority(path)) == null) {
        reader.put(getAuthority(path), new RawFileReader(path))
      }

      val stream = reader
        .get(getAuthority(path))
        .deleteFile(path)
        new ScrayListenableFuture(Unit)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        new ScrayListenableFuture(e)
      }
    }

  }
  private def getAuthority(path: String): String = {
    val uri = new URI(path)
    println(uri.getAuthority)
    uri.getAuthority
  }
}