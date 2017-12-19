package scray.hdfs.coordination

import scray.hdfs.index.format.sequence.SequenceFileWriter
import java.util.UUID
import com.typesafe.scalalogging.LazyLogging

class CoordinatedWriter(basePath: String, queryspace: String) extends LazyLogging {

  val reder = new ReadWriteCoordinatorImpl(basePath)
  reder.registerNewWriteDestination(queryspace)

  var writer: Option[SequenceFileWriter] = reder.getWriteDestination(queryspace).map(filePath => {
    new SequenceFileWriter(filePath + UUID.randomUUID())
  })

  def write(key: String, data: String) {

    logger.debug("Write to " + reder.getWriteDestination(queryspace))

    writer.get.insert(key, System.currentTimeMillis(), data.getBytes)
  }

  def nextVersion = {
    writer.map(_.close)

    reder.switchToNextVersion(queryspace)

    writer = reder.getWriteDestination(queryspace).map(filePath => {
      new SequenceFileWriter(filePath)
    })

  }

}