package scray.hdfs.io.index.format.example

import java.io.ByteArrayInputStream
import java.math.BigInteger

import scray.hdfs.io.coordination.CoordinatedWriter
import scray.hdfs.io.coordination.Version
import scray.hdfs.io.coordination.WriteDestination
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable
import scray.hdfs.io.write.IHdfsWriterConstats



object CoordinatedWriterExample {
  
  def main(args: Array[String]) {

    val metadata = WriteDestination("000", "target/CoordinateWriteExample", IHdfsWriterConstats.FileFormat.SequenceFile_Text_BytesWritable, Version(0), 64 * 1024 * 1024L, 5)
    val writer = new CoordinatedWriter(8192, metadata, new OutputTextBytesWritable)

    println(metadata.maxNumberOfInserts)
    
    for(i <- 0 to 50) {
          val id = i + ""
          val time = System.currentTimeMillis()
          val stream =  new ByteArrayInputStream(s"Hallo ${i}".getBytes)
          val size = new BigInteger("12")
      writer.insert(id, time, stream, size, 64 * 1024 * 1024)
    }
  }
}