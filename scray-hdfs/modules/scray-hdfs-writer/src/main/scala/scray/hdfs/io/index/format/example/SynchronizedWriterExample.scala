package scray.hdfs.io.index.format.example

import java.io.ByteArrayInputStream
import java.math.BigInteger

import scray.hdfs.io.coordination.CoordinatedWriter
import scray.hdfs.io.configure.Version
import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable
import scray.hdfs.io.write.IHdfsWriterConstats
import java.util.Optional



object CoordinatedWriterExample {
  
  def main(args: Array[String]) {

    val metadata = WriteParameter("000", "target/CoordinateWriteExample", Optional.empty(), IHdfsWriterConstats.SequenceKeyValueFormat.SequenceFile_Text_BytesWritable, Version(0), false, 64 * 1024 * 1024L, 5)
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