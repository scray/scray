package scray.hdfs.io.index.format.example

import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.osgi.WriteServiceImpl
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scala.util.Random
import scray.hdfs.io.configure.RandomUUIDFilenameCreator
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.Level
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import java.util.concurrent.Executors
import java.util.UUID
import scray.hdfs.io.write.WriteService

object ConcurrentWriteExample extends LazyLogging {
  def main(args: Array[String]) {
    
    if (args.size != 1) {
      println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/abc.seq")
    } else {
      val writeService = new WriteServiceImpl

      val config = new WriteParameter.Builder()
        .setPath(args(0))
        .setFileFormat(SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT)
        .setMaxFileSize(3000)
        .setTimeLimit(100)
        .setFileNameCreator(new RandomUUIDFilenameCreator())
        .createConfiguration

      val writeId1 = writeService.createWriter(config)
      val writeId2 = writeService.createWriter(config)
      val writeId3 = writeService.createWriter(config)

      val executor = Executors.newFixedThreadPool(40);

      for (i <- 0 to 20000) {

        executor.execute(createTask(writeId1, writeService))
        executor.execute(createTask(writeId2, writeService))
        executor.execute(createTask(writeId3, writeService))

        Thread.sleep(10)
      }
      
      writeService.close(writeId1)
      writeService.close(writeId1)
      writeService.close(writeId1)
    }
  }

  def createTask(writerId: UUID, writeService: WriteService): Runnable = {
    new Runnable() {
      def run() {
        writeService.insert(writerId, "id42", System.currentTimeMillis(), "{data: 123}".getBytes).get.getBytesInserted()
      }
    }
  }
}