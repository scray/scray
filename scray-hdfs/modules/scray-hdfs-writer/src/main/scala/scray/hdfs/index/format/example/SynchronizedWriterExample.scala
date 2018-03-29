package scray.hdfs.index.format.example

import scray.hdfs.coordination.CoordinatedWriter
import scray.hdfs.coordination.ReadWriteCoordinatorImpl
import scray.hdfs.coordination.IHdfsWriterConstats
import scray.hdfs.coordination.WriteDestination
import scray.hdfs.coordination.Version
import java.io.ByteArrayInputStream
import java.math.BigInteger


object CoordinatedWriterExample {
  
  def main(args: Array[String]) {
    val writerRegistry = new ReadWriteCoordinatorImpl

    val metadata = WriteDestination("000", "hdfs://bdq-cassandra4.seeburger.de/bisTest/", IHdfsWriterConstats.FileFormat.SequenceFile, Version(0), 64 * 1024 * 1024L, 5)
    println(metadata.maxNumberOfInserts)
    val writer = writerRegistry.getWriter(metadata)
    
    for(i <- 0 to 50) {
          val id = i + ""
          val time = System.currentTimeMillis()
          val stream =  new ByteArrayInputStream(s"Hallo ${i}".getBytes)
          val size = new BigInteger("12")
      writer.insert(id, time, stream, size, 64 * 1024 * 1024)
    }
  }
}