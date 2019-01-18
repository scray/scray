package scray.hdfs.io.index.format.example

import scray.hdfs.io.osgi.WriteServiceImpl
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat

object ServiceApiExample {
  def main(args: Array[String]) {

    val writeService = new WriteServiceImpl

    val writeId = writeService.createWriter("hdfs://host1.scray.org/ra", SequenceKeyValueFormat.SequenceFile_Text_Text, 0, "ggga.txt")

    for (i <- 0 to 10) {
      println(writeService.insert(writeId, "id42", System.currentTimeMillis(), createDataElement.getBytes).get.getBytesInserted())
    }
    writeService.close(writeId)
  }

  def createDataElement: String = {
    "{\"name\": \"station-" + Double.box(Math.random() * 500).intValue() + "\", " +
    "\"mode\": \"" + Double.box(Math.random() * 2).intValue() + "\"," +
    "\"temperature\": \"" + Math.floor(Math.random() * 200) + "\"," +
    "\"energyConsumption\": \"" + Math.random() * 100 + "\"," + 
    " \"skill_name\": \"REX_Waiting\"}"
  }
}