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

    val conf = new (WriteParameter.Builder)
    .setPath("target/CoordinateWriteExample")
    .setFileFormat(IHdfsWriterConstats.SequenceKeyValueFormat.SequenceFile_Text_BytesWritable)
    .setMaxNumberOfInserts(5)
    .createConfiguration
    val writer = new CoordinatedWriter(8192, conf, new OutputTextBytesWritable)

    println(conf.maxNumberOfInserts)
    
    for(i <- 0 to 50) {
          val id = i + ""
          val time = System.currentTimeMillis()
          val stream =  new ByteArrayInputStream(s"Hallo ${i}".getBytes)
          val size = new BigInteger("12")
      writer.insert(id, time, stream, size, 64 * 1024 * 1024)
    }
  }
}