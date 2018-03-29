//package scray.hdfs.index.format
//
//object Main {
//
//  def main(args: Array[String]): Unit = {
//    val writer = new Buffer(10000, "hdfs://10.11.22.41:8020/bdq-blob/")
//    val converter = new DataConverter
//
//    var positon = 0L
//
//    var start = 0L
//    var end = 0L;
//    
//    for (i <- 0 to 1000000) {
//
//
//      start = System.currentTimeMillis()
//      
//      val key = s"key${i}".getBytes("UTF8")
//      val value = s"val${i}".getBytes("UTF8")
//      val dataIdxRecord = converter.createDataAndIndexRecord(key, value, )
//      
//      end = System.currentTimeMillis()
//
//      println(end - start)
//      //writer.addValue((dataIdxRecord._1, dataIdxRecord._2));
//    }
//    
//  }
//}