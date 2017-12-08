package scray.hdfs.index.format.example

import scray.hdfs.index.format.orc.ORCFileReader

object ReadExampleOrcFile {
   def main(args: Array[String]) {

    if (args.size == 0) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
    } else {

      val reader = new ORCFileReader(s"${args(0)}/scray-data.orc}")
      
      reader.get("key_42").map(data => {
        println("===================================")
        println("Received data for key_42: " + new String(data))
        println("===================================")
      })
      
    }
   }
}