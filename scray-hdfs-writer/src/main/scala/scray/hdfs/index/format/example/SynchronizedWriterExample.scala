package scray.hdfs.index.format.example

import scray.hdfs.coordination.CoordinatedWriter


object CoordinatedWriterExample {
  
  def main(args: Array[String]) {
    val writer = new CoordinatedWriter("/tmp/scray-hdfs", "000")
    
    for(i <- 0 to 100) {
      writer.write(s"k${i}", s"d${i}")
    }
    
    writer.nextVersion
    
    for(i <- 0 to 100) {
      writer.write(s"k${i}", s"d${i}")
    }
    
    writer.nextVersion
  }
}