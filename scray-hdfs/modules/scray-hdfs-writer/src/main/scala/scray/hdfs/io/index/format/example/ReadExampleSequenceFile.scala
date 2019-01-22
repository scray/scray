package scray.hdfs.io.index.format.example

import scray.hdfs.io.index.format.sequence.SequenceFileWriter
import scray.hdfs.io.index.format.sequence.IdxReader
import scray.hdfs.io.index.format.sequence.ValueFileReader
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable
import scray.hdfs.io.index.format.sequence.RawValueFileReader

object ReadExampleSequenceFile {

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://host1.scray.org/magna/avro/sequence/066d8b48-0970-4abc-a6b8-0bdd1922f72a_RAW_I21826412_O-2135922676.seq")
    } else {

     val reader = new RawValueFileReader(s"${args(0)}", new OutputTextBytesWritable)

      while(reader.hasNext) {  
        val data = reader.next().get.get    
      }
      
      reader.close
    }
  }
}