package scray.hdfs.io.modify

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.write.WriteResult

class Renamer {
  
  def rename(source: String, destination: String, conf: Configuration = new Configuration()): ScrayListenableFuture = {
    try {
      val fs = FileSystem.get(URI.create(source), conf);
      fs.rename(new Path(source), new Path(destination));
      fs.close()
      new ScrayListenableFuture(new WriteResult(true, s"File ${source} renamed to ${destination}"))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }
}