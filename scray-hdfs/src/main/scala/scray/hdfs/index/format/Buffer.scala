package scray.hdfs.index.format

import java.util.ArrayList
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.concurrent.Executors

class Buffer(batchSize: Int, path: String) extends LazyLogging {
  
  var idxBuffer = new ArrayList[IndexFileRecord](batchSize)
  var dataBuffer = new ArrayList[BlobFileRecord](batchSize)
  
  val writerExecutor = Executors.newFixedThreadPool(5);

  var datafilePossition = 0L // Byte possition in data file.
  
  def addValue(value: Tuple2[BlobFileRecord, IndexFileRecord]) = synchronized  {

    idxBuffer.add(value._2)
    dataBuffer.add(value._1)
    
    if (idxBuffer.size() == batchSize) {
      
      // Write full buffer
      writerExecutor.submit(new HDFSWriter(path, idxBuffer)).get
      
      // Provide empty buffer
      idxBuffer = new ArrayList[IndexFileRecord](batchSize)
    }
    
    if (dataBuffer.size() == batchSize) {
      logger.debug(s"Flush to HDFS")
      
      // Write full buffer
      writerExecutor.submit(new HDFSWriter(path, dataBuffer)).get
      
      // Provide empty buffer
      dataBuffer = new ArrayList[BlobFileRecord](batchSize)
    }
  }
  
}