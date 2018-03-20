package scray.hdfs.index.format.example

import scray.hdfs.index.format.sequence.SequenceFileWriter
import scray.hdfs.index.format.sequence.IdxReader
import scray.hdfs.index.format.sequence.BlobFileReader

object ReadExampleSequenceFile {

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
    } else {

     val idxReader = new IdxReader(s"${args(0)}.idx")
     val blobReader = new BlobFileReader(s"${args(0)}.blob")

      while(idxReader.hasNext) {
        
        val idx = idxReader.next().get
        println("idx" + idx)
        println("Data: \t" + new String(blobReader.getNextBlob(idx.getKey, 1, idx.getPosition).get._2.getData))
        println("\n")
      }
      
      idxReader.close
      blobReader.close
    }
  }
}