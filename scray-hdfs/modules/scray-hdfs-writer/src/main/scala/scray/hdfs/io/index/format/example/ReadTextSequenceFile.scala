package scray.hdfs.io.index.format.example

import scray.hdfs.io.configure.RandomUUIDFilenameCreator
import scray.hdfs.io.osgi.ReadServiceImpl
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scray.hdfs.io.configure.WriteParameter

object ReadTextSequenceFile {
  def main(args: Array[String]) {

    if (args.size != 1) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/abc.seq")
    } else {
      val reader = new ReadServiceImpl

      val id = reader.readFullSequenceFile(args(0), SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT, System.getProperty("user.name"), "".getBytes)
      var count = 0L;
      while (reader.hasNextSequenceFilePair(id).get) {
        val res = new String(reader.getNextSequenceFilePair(id).get.getValue)

        if((count % 1000) == 0) {
          println(count + "\t" + res)
        }
        count += 1   
      }
      println(count)
    }

  }
}