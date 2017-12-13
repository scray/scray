package scray.hdfs.index.format.example

import scray.hdfs.index.format.sequence.SequenceFileWriter
import scray.hdfs.index.format.orc.ORCFileWriter

object WriteExampleOrcFile {

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
    } else {

      val writer = new ORCFileWriter(s"${args(0)}/scray-data.orc}")

      for (i <- 100 to 200) {
        val key = "key_" + i
        val value = "data_" + i

        println(s"Write key value data. key=${key}, value=${value}")

        writer.insert(key, System.currentTimeMillis(), value.getBytes)
      }

      writer.close
    }
  }
}