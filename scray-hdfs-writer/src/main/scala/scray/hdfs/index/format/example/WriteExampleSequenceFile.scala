package scray.hdfs.index.format.example

import scray.hdfs.index.format.sequence.SequenceFileWriter

object WriteExampleSequenceFile {

  def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
    } else {

      val writer = new SequenceFileWriter(s"${args(0)}/scray-data-${System.currentTimeMillis()}")

      for (i <- 0 to 100) {
        val key = "key_" + i
        val value = "data_" + i

        println(s"Write key value data. key=${key}, value=${value}")

        writer.insert(key, System.currentTimeMillis(), value.getBytes)
      }

      writer.close
    }
  }
}