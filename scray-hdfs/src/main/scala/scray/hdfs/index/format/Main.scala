package scray.hdfs.index.format

object Main {

  def main(args: Array[String]): Unit = {
    val writer = new Buffer(1000, "hdfs://192.168.0.201:8020/bdq-blob/")
    val converter = new DataConverter

    var positon = 0L

    
    for (i <- 0 to 1000000) {
      val key = s"key${i}".getBytes("UTF8")
      val value = s"val${i}".getBytes("UTF8")

      val dataIdxRecord = converter.createDataAndIndexRecord(key, value, positon)

      writer.addValue((dataIdxRecord._1, dataIdxRecord._2));
      
    }
    
  }
}