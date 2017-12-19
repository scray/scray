package scray.hdfs.index.format.example

import scray.hdfs.index.format.sequence.SequenceFileWriter
import scray.hdfs.index.format.sequence.IdxReader
import scray.hdfs.index.format.sequence.BlobFileReader

object ReadExampleSequenceFile {

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
    } else {

      //val writer = new SequenceFileWriter(s"${args(0)}/scray-data-${System.currentTimeMillis()}")
     val idxReader = new IdxReader(  s"${args(0)}/scray-data-1513170410431.idx")
     val writer = new BlobFileReader(s"${args(0)}/scray-data-1513170410431.blob")

      while(idxReader.hasNext) {
        
        println("\n")
        val idx = idxReader.next().get
        println("idx" + idx)
        println("Data: \t" + new String(writer.get(idx.getKey, idx.getPosition).get))

        // println("Data: \t" + new String(writer.get(idx.getKey, idx.getPosition).get))
        println("\n")
        return 
      }
        //println(s"Write key value data. key=${key}, value=${value}")
        //println(writer.get("key_746366", 3358913))

        //writer.insert(key, System.currentTimeMillis(), value.getBytes)

      //writer.close
    }
  }
}