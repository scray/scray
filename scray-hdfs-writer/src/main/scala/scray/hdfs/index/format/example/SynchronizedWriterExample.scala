package scray.hdfs.index.format.example

import scray.hdfs.coordination.CoordinatedWriter
import scray.hdfs.coordination.ReadWriteCoordinatorImpl
import scray.hdfs.coordination.IHdfsWriterConstats
import scray.hdfs.coordination.WriteDestination
import scray.hdfs.coordination.Version


object CoordinatedWriterExample {
  
  def main(args: Array[String]) {
    val writerRegistry = new ReadWriteCoordinatorImpl

    val metadata = WriteDestination("000", "hdfs://bdq-cassandra4.seeburger.de/bisTest/", IHdfsWriterConstats.FileFormat.SequenceFile, Version(0), 1 * 2048L)
    val writer = writerRegistry.getWriter(metadata)
    
    for(i <- 0 to 1000000) {
      writer.insert(i + "", System.currentTimeMillis(), s"Hallo ${i}".getBytes)
    }
  }
}