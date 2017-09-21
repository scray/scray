package scray.hdfs.index.format

import java.util.ArrayList
import java.util.concurrent.Executors
import java.util.concurrent.Future
import com.typesafe.scalalogging.LazyLogging

class Buffer(batchSize: Int, path: String) extends LazyLogging {

  var idxBuffer = new ArrayList[IndexFileRecord](batchSize)
  var previousIdxBufferWriterTask: Future[Boolean] = null

  var dataBuffer = new ArrayList[BlobFileRecord](batchSize)
  var previousDataBufferWriterTask: Future[Boolean] = null

  val writerExecutor = Executors.newFixedThreadPool(5);

  var datafilePossition = 0L // Byte possition in data file.

  
  def addValue(value: BlobFileRecord, flush: Boolean): Unit = synchronized {

    dataBuffer.add(value)
    if (dataBuffer.size() == batchSize || flush) {
      flushData
    }
  }

  def addValue(value: IndexFileRecord, flush: Boolean): Unit = synchronized {

    idxBuffer.add(value)

    if (idxBuffer.size() == batchSize || flush) {
      flushIdx
    }
  }

  def addValue(value: Tuple2[BlobFileRecord, IndexFileRecord], flush: Boolean): Unit = synchronized {
    this.addValue(value._1, flush)
    this.addValue(value._2, flush)
  }

  def flushIdx = synchronized {
    if (previousIdxBufferWriterTask != null) {
      previousIdxBufferWriterTask.get
    }

    // Write full buffer
    previousIdxBufferWriterTask = writerExecutor.submit(new HDFSWriter(path, idxBuffer))

    // Provide empty buffer
    idxBuffer = new ArrayList[IndexFileRecord](batchSize)
    
    if (previousIdxBufferWriterTask != null) {
      previousIdxBufferWriterTask.get
    }

  }

  def flushData = synchronized {
    logger.debug(s"Flush to HDFS")

    // Starte next write process if previous completed
    if (previousDataBufferWriterTask != null) {
      previousDataBufferWriterTask.get
    }

    // Write full buffer
    previousDataBufferWriterTask = writerExecutor.submit(new HDFSWriter(path, dataBuffer))

    // Provide empty buffer
    dataBuffer = new ArrayList[BlobFileRecord](batchSize)
  }

  def flush = {
    flushIdx
    flushData
  }

}