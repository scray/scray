package scray.hdfs.index.format

import java.util.ArrayList

class Buffer(batchSize: Int) {
  
  var idxBuffer = new ArrayList[IndexFileRecord](batchSize)
  var dataBuffer = new ArrayList[BlobFileRecord](batchSize)

  var datafilePossition = 0L // Byte possition in data file.
  
  def addValue(value: Tuple2[BlobFileRecord, IndexFileRecord]) {

    if (batchBuffer.size() == batchSize) {
      logger.debug(s"Flush to HDFS")
      this.writeIdx

      println("Re update buffer")
      batchBuffer = new ArrayList[Tuple2[BlobFileRecord, IndexFileRecord]](batchSize)
    }

    batchBuffer.add(value)
  }
  
}