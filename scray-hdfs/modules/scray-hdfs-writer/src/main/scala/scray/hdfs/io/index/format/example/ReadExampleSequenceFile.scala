package scray.hdfs.io.index.format.example

import scray.hdfs.io.index.format.sequence.BinarySequenceFileWriter
import scray.hdfs.io.index.format.sequence.IdxReader
import scray.hdfs.io.index.format.sequence.BlobFileReader
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob

object ReadExampleSequenceFile {

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
    } else {

     val idxReader = new IdxReader(s"${args(0)}.idx", new OutputBlob)
     val blobReader = new BlobFileReader(s"${args(0)}.blob")

      while(idxReader.hasNext) {
        
        val idx = idxReader.next().get
        println("idx" + idx)
        println("Data: \t" + new String(blobReader.getNextBlob(idx.getKey, 0, idx.getPosition).get._2.getData))
        println("\n")
      }
      
      idxReader.close
      blobReader.close
    }
  }
}