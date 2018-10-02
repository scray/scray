package scray.hdfs.io.index.format.example

import scray.hdfs.io.osgi.WriteServiceImpl
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat

object ServiceApiExample {
    def main(args: Array[String]) {
      
    val writeService = new WriteServiceImpl
    
    val writeId = writeService.createWriter("hdfs://10.0.103.102/LogTest3", SequenceKeyValueFormat.SequenceFile_Text_Text)
    
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc1".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc2".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc3".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc4".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc5".getBytes).get
    
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc6".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc7".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc8".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc9".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc10".getBytes).get
    
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc11".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc12".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc13".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc14".getBytes).get
    writeService.insert(writeId, "123", System.currentTimeMillis(), "Abc15".getBytes).get

    writeService.close(writeId)
  }
}