package scray.hdfs.index.format.example

import scray.hdfs.index.format.sequence.TextSequenceFileWriter

object TextToTextSequenceFiles {
  
  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage TextFilesToTextSequenceFiles SOURCE DEST")
      println("Example parameters:\n  hdfs://10.0.0.1/SequenceTest/testFile /home/stefan/Downloads/testFile.pdf")
    } else {
      
      val writer = new TextSequenceFileWriter(args(1))
      
      writer.insert("id1", """{"msg_id": 1, "msg": "msg1"}""")
      writer.insert("id2", """{"msg_id": 2, "msg": "msg2"}""")
      writer.insert("id3", """{"msg_id": 3, "msg": "msg3"}""")
      
      writer.close
      
    }
  }
}