package scray.hdfs.index.format.example





import org.apache.hadoop.fs.FileSystem

import scray.hdfs.index.format.raw.RawFileWriter
import java.io.ByteArrayInputStream


object Main {
  def main(args: Array[String]) {
    val writer = new RawFileWriter("hdfs://host1.scray.org")
    writer.write("/tmp/ard/ff", new ByteArrayInputStream("Chicken".getBytes))

  }
  
}