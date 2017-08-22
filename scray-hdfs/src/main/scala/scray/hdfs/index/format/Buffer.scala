package scray.hdfs.index.format

import java.util.ArrayList
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.concurrent.Executors
import java.util.concurrent.Future

class Buffer(batchSize: Int, path: String) extends LazyLogging {
  
  var idxBuffer = new ArrayList[IndexFileRecord](batchSize)
  var previousIdxBufferWriterTask: Future[Boolean] = null
  
  var dataBuffer = new ArrayList[BlobFileRecord](batchSize)
  var previousDataBufferWriterTask: Future[Boolean] = null

  val writerExecutor = Executors.newFixedThreadPool(5);

  var datafilePossition = 0L // Byte possition in data file.
  
  def addValue(value: Tuple2[BlobFileRecord, IndexFileRecord]) = synchronized  {

    idxBuffer.add(value._2)
    dataBuffer.add(value._1)
    
    if (idxBuffer.size() == batchSize) {
      
      if(previousIdxBufferWriterTask != null) {
        previousDataBufferWriterTask.get
      }
      
      // Write full buffer
      previousIdxBufferWriterTask = writerExecutor.submit(new HDFSWriter(path, idxBuffer))
      
      // Provide empty buffer
      idxBuffer = new ArrayList[IndexFileRecord](batchSize)
    }
    
    if (dataBuffer.size() == batchSize) {
      logger.debug(s"Flush to HDFS")
      
      // Starte next write process if previous completed
      if(previousDataBufferWriterTask != null) {
        previousDataBufferWriterTask.get
      }
      
      // Write full buffer
      previousDataBufferWriterTask = writerExecutor.submit(new HDFSWriter(path, dataBuffer))
      
      // Provide empty buffer
      dataBuffer = new ArrayList[BlobFileRecord](batchSize)
    }
  }
  
}