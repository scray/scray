package scray.hdfs.io.index.format.example



import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.osgi.WriteServiceImpl
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scala.util.Random
import scray.hdfs.io.configure.RandomUUIDFilenameCreator
import java.io.ByteArrayInputStream

object ServiceApiExample {
  def main(args: Array[String]) {

    val writeService = new WriteServiceImpl
    
    val config = new WriteParameter.Builder()
    .setPath("hdfs://host1.scray.org/tmp/customFile/ff/")
    .setFileFormat(SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT)
    .setTimeLimit(1)
    .setFileNameCreator(new RandomUUIDFilenameCreator())
    .createConfiguration

    val writeId = writeService.createWriter(config)

    for(i <- 0 to 100) {
      writeService.writeRawFile("hdfs://host1.scray.org/tmp/argo9/" + (Math.random() * 20).toInt + "/" + System.currentTimeMillis() , new ByteArrayInputStream("eeee5555555555555".getBytes), "hdfs", "".getBytes).get
      
      println("write")
      //Thread.sleep(10)
    }
    
    while(true) {
      Thread.sleep(10)
    }
  }

  def createDataElement: String = {
    "{\"name\": \"station-" + Double.box(Math.random() * 500).intValue() + "\", " +
    "\"mode\": \"" + Double.box(Math.random() * 2).intValue() + "\"," +
    "\"temperature\": \"" + Math.floor(Math.random() * 200) + "\"," +
    "\"energyConsumption\": \"" + Math.random() * 100 + "\"," + 
    " \"skill_name\": \"REX_Waiting\"}"
  }
}